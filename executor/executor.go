package executor

import (
	"bytes"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/mapred"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util"
	"github.com/naturali/kmr/util/log"

	"github.com/reusee/mmh3"
)

const (
	ReducerConcurrentLevel = 16
)

type ComputeWrapClass struct {
	mapper   mapred.Mapper
	reducer  mapred.Reducer
	combiner mapred.Combiner
}

func (cw *ComputeWrapClass) BindMapper(mapper mapred.Mapper) {
	cw.mapper = mapper
}

func (cw *ComputeWrapClass) BindReducer(reducer mapred.Reducer) {
	cw.reducer = reducer
}

func (cw *ComputeWrapClass) BindCombiner(combiner mapred.Combiner) {
	cw.combiner = combiner
}

func (cw *ComputeWrapClass) sortAndCombine(aggregated []*records.Record) []*records.Record {
	sort.Slice(aggregated, func(i, j int) bool {
		return bytes.Compare(aggregated[i].Key, aggregated[j].Key) < 0
	})
	if cw.combiner == nil {
		return aggregated
	}
	keyClass, valueClass := cw.combiner.GetInputKeyTypeConverter(), cw.combiner.GetInputValueTypeConverter()
	combined := make([]*records.Record, 0)
	var curRecord *records.Record
	var valueInterface interface{}
	var keyInterface interface{}
	outputFunc := func(v interface{}) {
		valueInterface = v
	}
	for _, r := range aggregated {
		if curRecord == nil {
			curRecord = r
			keyInterface = keyClass.FromBytes(r.Key)
			valueInterface = valueClass.FromBytes(r.Value)
			continue
		}
		if !bytes.Equal(curRecord.Key, r.Key) {
			if curRecord.Key != nil {
				curRecord.Value = valueClass.ToBytes(valueInterface)
				combined = append(combined, curRecord)
			}
			curRecord = r
			keyInterface = keyClass.FromBytes(r.Key)
			valueInterface = valueClass.FromBytes(r.Value)
		} else {
			cw.combiner.Combine(keyInterface, valueInterface, valueClass.FromBytes(r.Value), outputFunc)
		}
	}
	if curRecord != nil && curRecord.Key != nil {
		curRecord.Value = valueClass.ToBytes(valueInterface)
		combined = append(combined, curRecord)
	}
	return combined
}

func (cw *ComputeWrapClass) DoMap(rr records.RecordReader, writers []records.RecordWriter, flushBucket bucket.Bucket, flushSize int, mapID int, nReduce int, workerID int64) (err error) {
	maxNumConcurrentFlush := 2
	startTime := time.Now()
	aggregated := make([]*records.Record, 0)
	flushOutFiles := make([]string, 0)
	currentAggregatedSize := 0

	// map
	waitc := make(chan struct{})
	inputKV := make(chan *kmrpb.KV, 1024)
	outputKV := make(chan *kmrpb.KV, 1024)
	cw.mapper.Init()
	keyClass, valueClass := cw.mapper.GetInputKeyTypeConverter(), cw.mapper.GetInputValueTypeConverter()
	go func() {
		for kvpair := range inputKV {
			key := keyClass.FromBytes(kvpair.Key)
			value := valueClass.FromBytes(kvpair.Value)
			//TODO: implement report
			collectFunc := func(k, v interface{}) {
				keyBytes := cw.mapper.GetOutputKeyTypeConverter().ToBytes(k)
				valueBytes := cw.mapper.GetOutputValueTypeConverter().ToBytes(v)
				outputKV <- &kmrpb.KV{Key: keyBytes, Value: valueBytes}
			}
			cw.mapper.Map(key, value, collectFunc, nil)
		}
		close(outputKV)
	}()
	go func() {
		sem := util.NewSemaphore(maxNumConcurrentFlush)
		var waitFlushWrite sync.WaitGroup
		for in := range outputKV {
			aggregated = append(aggregated, KVToRecord(in))
			currentAggregatedSize += 8 + len(in.Key) + len(in.Value)
			if currentAggregatedSize >= flushSize*1024*1024 {
				filename := bucket.FlushoutFileName("map", mapID, len(flushOutFiles), workerID)
				sem.Acquire(1)
				go func(filename string, data []*records.Record) {
					writer, err := flushBucket.OpenWrite(filename)
					recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
					if err != nil {
						log.Fatal(err)
					}
					for _, r := range cw.sortAndCombine(data) {
						if err := recordWriter.WriteRecord(r); err != nil {
							log.Fatal(err)
						}
					}
					if err := recordWriter.Close(); err != nil {
						log.Fatal(err)
					}
					sem.Release(1)
				}(filename, aggregated)
				aggregated = make([]*records.Record, 0)
				currentAggregatedSize = 0
				flushOutFiles = append(flushOutFiles, filename)
			}
		}
		sem.Acquire(maxNumConcurrentFlush)
		aggregated = cw.sortAndCombine(aggregated)
		waitFlushWrite.Wait()
		close(waitc)
	}()
	for rr.HasNext() {
		inputKV <- RecordToKV(rr.Pop())
	}
	close(inputKV)
	<-waitc
	log.Debug("DONE Map. Took:", time.Since(startTime))

	readers := make([]records.RecordReader, 0)
	for _, file := range flushOutFiles {
		reader, err := flushBucket.OpenRead(file)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		recordReader := records.MakeRecordReader("stream", map[string]interface{}{"reader": reader})
		readers = append(readers, recordReader)
	}
	readers = append(readers, records.MakeRecordReader("memory", map[string]interface{}{"data": aggregated}))

	sorted := make(chan *records.Record, 1024)
	go records.MergeSort(readers, sorted)

	var curRecord *records.Record
	combinerKeyClass, combinerValueClass :=
		cw.combiner.GetInputKeyTypeConverter(), cw.combiner.GetInputValueTypeConverter()
	outputFunc := func(v interface{}) {
		curRecord.Value = combinerValueClass.ToBytes(v)
	}
	for r := range sorted {
		if curRecord == nil {
			curRecord = r
			continue
		}
		if cw.combiner == nil || !bytes.Equal(curRecord.Key, r.Key) {
			if curRecord.Key != nil {
				rBucketID := util.HashBytesKey(curRecord.Key) % nReduce
				writers[rBucketID].WriteRecord(curRecord)
			}
			curRecord = r
		} else {
			cw.combiner.Combine(
				combinerKeyClass.FromBytes(curRecord.Key), combinerValueClass.FromBytes(curRecord.Value),
				combinerValueClass.FromBytes(r.Value), outputFunc)
		}
	}
	if curRecord != nil && curRecord.Key != nil {
		rBucketID := util.HashBytesKey(curRecord.Key) % nReduce
		writers[rBucketID].WriteRecord(curRecord)
	}

	for _, reader := range readers {
		reader.Close()
	}
	// Delete flushOutFiles
	for _, file := range flushOutFiles {
		flushBucket.Delete(file)
	}
	log.Debug("FINISH Write IntermediateFiles. Took:", time.Since(startTime))
	return
}

// doReduce does reduce operation
func (cw *ComputeWrapClass) DoReduce(readers []records.RecordReader, writer records.RecordWriter) error {
	startTime := time.Now()
	sorted := make(chan *records.Record, 1024)
	go records.MergeSort(readers, sorted)

	readWhich := make(chan uint32, 1024)
	var inputsArr [ReducerConcurrentLevel]chan *kmrpb.KV
	var outputsArr [ReducerConcurrentLevel]chan *kmrpb.KV
	endOfKeyGuard := &kmrpb.KV{Key: []byte{}, Value: []byte{}}

	cw.reducer.Init()
	reducerCaller := func(inputs <-chan *kmrpb.KV, outputs chan<- *kmrpb.KV, wg *sync.WaitGroup) {
		keyClass, valueClass := cw.reducer.GetInputKeyTypeConverter(), cw.reducer.GetInputValueTypeConverter()
		for {
			startKVPair := <-inputs
			if startKVPair == nil {
				break
			}
			if startKVPair == endOfKeyGuard {
				log.Fatal("Start KV pair should not be end of key guard")
			}
			key := keyClass.FromBytes(startKVPair.Key)
			reducerIteratedAllValue := false
			nextIter := &ValueIteratorFunc{
				IterFunc: func() (interface{}, error) {
					if reducerIteratedAllValue {
						return nil, errors.New(mapred.ErrorNoMoreKey)
					}
					if startKVPair != nil {
						tmp := startKVPair.Value
						startKVPair = nil
						return valueClass.FromBytes(tmp), nil
					}
					kvpair := <-inputs
					if kvpair != endOfKeyGuard && kvpair != nil {
						return valueClass.FromBytes(kvpair.Value), nil
					}
					reducerIteratedAllValue = true
					if kvpair == nil {
						return nil, errors.New(mapred.ErrorNoMoreInput)
					}
					return nil, errors.New(mapred.ErrorNoMoreKey)
				},
			}
			alreadyOutput := false
			collectFunc := func(v interface{}) {
				if alreadyOutput {
					log.Errorf("value of key: %v has been collected", key)
					return
				}
				keyBytes := cw.reducer.GetOutputKeyTypeConverter().ToBytes(key)
				valueBytes := cw.reducer.GetOutputValueTypeConverter().ToBytes(v)
				outputs <- &kmrpb.KV{Key: keyBytes, Value: valueBytes}
				alreadyOutput = true
			}
			cw.reducer.Reduce(key, nextIter, collectFunc, nil)
			if !alreadyOutput {
				outputs <- nil
			}
			for !reducerIteratedAllValue {
				nextIter.IterFunc()
			}
		}
		wg.Done()
	}

	var wg sync.WaitGroup
	var wgForReducerCaller sync.WaitGroup
	wg.Add(1)
	go func() {
		for idx := range readWhich {
			r := <-outputsArr[idx]
			if r != nil {
				writer.WriteRecord(KVToRecord(r))
			}
		}
		wg.Done()
	}()

	for i := range inputsArr {
		inputsArr[i] = make(chan *kmrpb.KV, 1024)
		outputsArr[i] = make(chan *kmrpb.KV, 1024)
		wgForReducerCaller.Add(1)
		go reducerCaller(inputsArr[i], outputsArr[i], &wgForReducerCaller)
	}

	var prevKey []byte
	var k uint32
	for r := range sorted {
		prevK := k
		if ReducerConcurrentLevel == 1 {
			k = 0
		} else {
			k = mmh3.Hash32(r.Key) % ReducerConcurrentLevel
		}
		if !bytes.Equal(prevKey, r.Key) {
			if prevKey != nil {
				inputsArr[prevK] <- endOfKeyGuard
				readWhich <- prevK
			}
			prevKey = r.Key
		}
		inputsArr[k] <- RecordToKV(r)
	}
	readWhich <- k

	for i := range inputsArr {
		inputsArr[i] <- nil
		close(inputsArr[i])
	}
	close(readWhich)

	wgForReducerCaller.Wait()

	for i := range outputsArr {
		close(outputsArr[i])
	}

	wg.Wait()
	log.Debug("DONE Reduce. Took:", time.Since(startTime))
	return nil
}

// RecordToKV converts an Record to a kmrpb.KV
func RecordToKV(record *records.Record) *kmrpb.KV {
	return &kmrpb.KV{Key: record.Key, Value: record.Value}
}

// KVToRecord converts a kmrpb.KV to an Record
func KVToRecord(kv *kmrpb.KV) *records.Record {
	return &records.Record{Key: kv.Key, Value: kv.Value}
}

// ValueIteratorFunc use a function as iterator interface
type ValueIteratorFunc struct {
	IterFunc func() (interface{}, error)
}

// Next call IterFunc directly
func (vif *ValueIteratorFunc) Next() (interface{}, error) {
	return vif.IterFunc()
}
