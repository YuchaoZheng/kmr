package executor_new

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/job"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/master"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util"
	"github.com/naturali/kmr/util/log"
)

var (
	jobName    = flag.String("jobname", "wc", "jobName")
	inputFile  = flag.String("file", "", "input file path")
	dataDir    = flag.String("intermediate-dir", "/tmp/", "directory of intermediate files")
	phase      = flag.String("phase", "", "map or reduce")
	nMap       = flag.Int("nMap", 1, "number of mappers")
	nReduce    = flag.Int("nReduce", 1, "number of reducers")
	mapID      = flag.Int("mapID", 0, "mapper id")
	reduceID   = flag.Int("reduceID", 0, "reducer id")
	flushSize  = flag.Int("flushSize", 100, "flushout size in megabytes")
	readerType = flag.String("reader-type", "textfile", "type of record reader for input files")

	masterAddr = flag.String("master-addr", "", "the address of master")
)

func getCollectFunc(outClass mapred.OutputClassBase, outputKV chan<- *kmrpb.KV) func(k, v interface{}) {
	return func(k, v interface{}) {
		keyBytes := outClass.GetOutputKeyClass().ToBytes(k)
		valueBytes := outClass.GetOutputValueClass().ToBytes(v)
		outputKV <- &kmrpb.KV{Key: keyBytes, Value: valueBytes}
	}
}

type ComputeWrapClass struct {
	mapClass    mapred.Mapper
	reduceClass mapred.Reducer
	// combineClass mapred.Reducer
	combineFunc func(v1 []byte, v2 []byte) []byte
}

func (cw *ComputeWrapClass) BindMapper(mapper mapred.Mapper) {
	cw.mapClass = mapper
}

func (cw *ComputeWrapClass) BindReducer(reducer mapred.Reducer) {
	cw.reduceClass = reducer
}

func (cw *ComputeWrapClass) BindCombiner(combiner func(v1 []byte, v2 []byte) []byte) {
	cw.combineFunc = combiner
}

func (cw *ComputeWrapClass) doMap(rr records.RecordReader, bk bucket.Bucket, mapID int, nReduce int, workerID int64) (err error) {
	maxNumConcurrentFlush := 2
	startTime := time.Now()
	aggregated := make([]*records.Record, 0)
	flushOutFiles := make([]string, 0)
	currentAggregatedSize := 0

	// map
	waitc := make(chan struct{})
	inputKV := make(chan *kmrpb.KV, 1024)
	outputKV := make(chan *kmrpb.KV, 1024)
	// outputKV := cw.mapFunc(inputKV)
	cw.mapClass.BeforeRun()
	keyClass, valueClass := cw.mapClass.GetInputKeyClass(), cw.mapClass.GetInputValueClass()
	go func() {
		for kvpair := range inputKV {
			key := keyClass.FromBytes(kvpair.Key)
			value := valueClass.FromBytes(kvpair.Value)
			//TODO: implement report
			cw.mapClass.Map(key, value, getCollectFunc(cw.mapClass, outputKV), nil)
		}
		close(outputKV)
	}()
	go func() {
		sem := util.NewSemaphore(maxNumConcurrentFlush)
		var waitFlushWrite sync.WaitGroup
		for in := range outputKV {
			aggregated = append(aggregated, KVToRecord(in))
			currentAggregatedSize += 8 + len(in.Key) + len(in.Value)
			// if currentAggregatedSize >= *flushSize*1024*1024 {
			if currentAggregatedSize >= 1*1024*1024 {
				filename := bucket.FlushoutFileName("map", mapID, len(flushOutFiles), workerID)
				sem.Acquire(1)
				go func(filename string, data []*records.Record) {
					writer, err := bk.OpenWrite(filename)
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
		reader, err := bk.OpenRead(file)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		recordReader := records.MakeRecordReader("stream", map[string]interface{}{"reader": reader})
		readers = append(readers, recordReader)
	}
	readers = append(readers, records.MakeRecordReader("memory", map[string]interface{}{"data": aggregated}))
	sorted := make(chan *records.Record, 1024)
	go records.MergeSort(readers, sorted)

	writers := make([]records.RecordWriter, 0)
	for i := 0; i < nReduce; i++ {
		intermediateFileName := bucket.IntermediateFileName(mapID, i, workerID)
		writer, err := bk.OpenWrite(intermediateFileName)
		recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		writers = append(writers, recordWriter)
	}

	curRecord := &records.Record{}
	for r := range sorted {
		if cw.combineFunc == nil || !bytes.Equal(curRecord.Key, r.Key) {
			if curRecord.Key != nil {
				rBucketID := util.HashBytesKey(curRecord.Key) % nReduce
				writers[rBucketID].WriteRecord(curRecord)
			}
			curRecord = r
		} else {
			curRecord.Value = cw.combineFunc(curRecord.Value, r.Value)
		}
	}
	if curRecord.Key != nil {
		rBucketID := util.HashBytesKey(curRecord.Key) % nReduce
		writers[rBucketID].WriteRecord(curRecord)
	}

	// Delete flushOutFiles
	for _, reader := range readers {
		reader.Close()
	}
	for _, file := range flushOutFiles {
		bk.Delete(file)
	}
	for i := 0; i < nReduce; i++ {
		writers[i].Close()
	}

	log.Debug("FINISH Write IntermediateFiles. Took:", time.Since(startTime))
	return
}

// doReduce does reduce operation
func (cw *ComputeWrapClass) doReduce(interBk bucket.Bucket, reduceID int, nMap, batchSize int, commitMappers []int64, writer records.RecordWriter) error {
	startTime := time.Now()
	readers := make([]records.RecordReader, 0)
	nBatch := nMap / batchSize
	if nMap%batchSize > 0 {
		nBatch += 1
	}
	for i := 0; i < nBatch; i++ {
		reader, err := interBk.OpenRead(bucket.IntermediateFileName(i, reduceID, commitMappers[i]))
		recordReader := records.NewStreamRecordReader(reader)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, recordReader)
	}
	sorted := make(chan *records.Record, 1024)
	go records.MergeSort(readers, sorted)

	inputs := make(chan *kmrpb.KV, 1024)
	// outputs := cw.reduceFunc(inputs)
	outputs := make(chan *kmrpb.KV, 1024)
	cw.reduceClass.BeforeRun()
	keyClass, valueClass := cw.reduceClass.GetInputKeyClass(), cw.reduceClass.GetInputValueClass()
	go func() {
		var valueChan chan interface{}
		var prevKeyBytes []byte
		var wg sync.WaitGroup
		for kvpair := range inputs {
			if prevKeyBytes == nil || !bytes.Equal(prevKeyBytes, kvpair.Key) {
				if prevKeyBytes != nil {
					close(valueChan)
				}
				valueChan = make(chan interface{}, 128)
				key := keyClass.FromBytes(kvpair.Key)
				wg.Add(1)
				go func() {
					cw.reduceClass.Reduce(key, valueChan, getCollectFunc(cw.reduceClass, outputs), nil)
					wg.Done()
				}()
				prevKeyBytes = kvpair.Key[:]
			}
			//TODO: implement report
			valueChan <- valueClass.FromBytes(kvpair.Value)
		}
		close(valueChan)
		wg.Wait()
		close(outputs)
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func(outputs <-chan *kmrpb.KV) {
		for r := range outputs {
			writer.WriteRecord(KVToRecord(r))
		}
		wg.Done()
	}(outputs)
	for r := range sorted {
		inputs <- RecordToKV(r)
	}
	close(inputs)
	wg.Wait()
	log.Debug("DONE Reduce. Took:", time.Since(startTime))
	return nil
}

func (cw *ComputeWrapClass) sortAndCombine(aggregated []*records.Record) []*records.Record {
	sort.Slice(aggregated, func(i, j int) bool {
		return bytes.Compare(aggregated[i].Key, aggregated[j].Key) < 0
	})
	if cw.combineFunc == nil {
		return aggregated
	}
	combined := make([]*records.Record, 0)
	curRecord := &records.Record{}
	for _, r := range aggregated {
		if !bytes.Equal(curRecord.Key, r.Key) {
			if curRecord.Key != nil {
				combined = append(combined, curRecord)
			}
			curRecord = r
		} else {
			curRecord.Value = cw.combineFunc(curRecord.Value, r.Value)
		}
	}
	if curRecord.Key != nil {
		combined = append(combined, curRecord)
	}
	return combined
}

func (cw *ComputeWrapClass) aggregateValues(inputs <-chan *kmrpb.KV, values chan<- interface{}) {

}

func (cw *ComputeWrapClass) Run() {
	flag.Parse()

	if os.Getenv("KMR_MASTER_ADDRESS") != "" {
		addr := os.Getenv("KMR_MASTER_ADDRESS")
		masterAddr = &addr
	}

	if *masterAddr == "" {
		// Local Run
		var taskID int
		switch *phase {
		case "map":
			taskID = *mapID
		case "reduce":
			taskID = *reduceID
		}
		bucket := job.BucketDescription{
			BucketType: "filesystem",
			Config:     job.BucketConfig{"directory": *dataDir},
		}
		err := cw.phaseSelector(*jobName, *phase,
			bucket, bucket, bucket,
			[]string{*inputFile}, *nMap, *nReduce, *readerType, taskID, 0, make([]int64, *nMap), 1)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var retcode kmrpb.ReportInfo_ErrorCode
		// Distributed Mode
		cc, err := grpc.Dial(*masterAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatal("cannot connect to master", err)
		}
		masterClient := kmrpb.NewMasterClient(cc)
		for {
			task, err := masterClient.RequestTask(context.Background(), &kmrpb.RegisterParams{
				JobName: *jobName,
			})
			if err != nil || task.Retcode != 0 {
				log.Error(err)
				// TODO: random backoff
				time.Sleep(1 * time.Second)
				continue
			}
			taskInfo := task.Taskinfo
			timer := time.NewTicker(master.HEARTBEAT_TIMEOUT / 2)
			go func() {
				for range timer.C {
					// SendHeartBeat
					masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
						JobName:  *jobName,
						Phase:    taskInfo.Phase,
						TaskID:   taskInfo.TaskID,
						WorkerID: task.WorkerID,
						Retcode:  kmrpb.ReportInfo_DOING,
					})
				}
			}()
			var mapBucket, interBucket, reduceBucket job.BucketDescription
			json.Unmarshal([]byte(taskInfo.MapBucketJson), &mapBucket)
			json.Unmarshal([]byte(taskInfo.IntermediateBucketJson), &interBucket)
			json.Unmarshal([]byte(taskInfo.ReduceBucketJson), &reduceBucket)

			err = cw.phaseSelector(taskInfo.JobName, taskInfo.Phase,
				mapBucket, interBucket, reduceBucket, taskInfo.Files,
				int(taskInfo.NMap), int(taskInfo.NReduce), taskInfo.ReaderType, int(taskInfo.TaskID), task.WorkerID, taskInfo.CommitMappers, int(taskInfo.MapBatchSize))
			retcode = kmrpb.ReportInfo_FINISH
			if err != nil {
				log.Debug(err)
				retcode = kmrpb.ReportInfo_ERROR
			}
			timer.Stop()
			masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
				JobName:  *jobName,
				Phase:    taskInfo.Phase,
				TaskID:   taskInfo.TaskID,
				WorkerID: task.WorkerID,
				Retcode:  retcode,
			})
			// backoff
			if err != nil {
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (cw *ComputeWrapClass) phaseSelector(jobName string, phase string,
	mapBucket, interBucket, reduceBucket job.BucketDescription, files []string,
	nMap int, nReduce int, readerType string, taskID int, workerID int64,
	commitMappers []int64, mapBatchSize int) error {
	interBk, err := bucket.NewBucket(interBucket.BucketType, interBucket.Config)
	if err != nil {
		log.Fatalf("Fail to open bucket: %v", err)
	}
	switch phase {
	case "map":
		log.Infof("starting id%d mapper", taskID)
		mapBk, err := bucket.NewBucket(mapBucket.BucketType, mapBucket.Config)
		if err != nil {
			log.Fatalf("Fail to open mapbucket: %v", err)
		}
		readers := make([]records.RecordReader, 0)
		for _, file := range files {
			reader, err := mapBk.OpenRead(file)
			if err != nil {
				log.Fatalf("Fail to open object %s: %v", file, err)
			}
			recordReader := records.MakeRecordReader(readerType, map[string]interface{}{"reader": reader})
			readers = append(readers, recordReader)
		}
		batchReader := records.NewChainReader(readers)
		// Mapper
		if err := cw.doMap(batchReader, interBk, taskID, nReduce, workerID); err != nil {
			log.Fatalf("Fail to Map: %v", err)
		}
		batchReader.Close()
	case "reduce":
		log.Infof("starting id%d reducer", nReduce)
		reduceBk, err := bucket.NewBucket(reduceBucket.BucketType, reduceBucket.Config)
		if err != nil {
			log.Fatalf("Fail to open reducebucket: %v", err)
		}
		// Reduce
		outputFile := "res-" + strconv.Itoa(taskID) + ".t"
		writer, err := reduceBk.OpenWrite(outputFile)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
		if err = cw.doReduce(interBk, taskID, nMap, mapBatchSize, commitMappers, recordWriter); err != nil {
			log.Fatalf("Fail to Reduce: %v", err)
		}
		recordWriter.Close()
	default:
		panic("bad phase")
	}
	log.Info("Exit executor")
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
