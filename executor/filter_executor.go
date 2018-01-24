package executor

import (
	"fmt"
	"errors"
	"time"
	"io"
	"encoding/json"

	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util/log"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/mapred"
)

type FilterExecutor struct {
	ExecutorBase
}

func NewFilterExecutor() *FilterExecutor {
	return &FilterExecutor{}
}

func (e *FilterExecutor) isTargetFor(n jobgraph.TaskNode) bool {
	_, ok := n.(*jobgraph.FilterNode)
	return ok
}

func (e *FilterExecutor) handleTaskNode(info kmrpb.TaskInfo, n jobgraph.TaskNode) (err error) {
	fNode, _ := n.(*jobgraph.FilterNode)
	if fNode == nil {
		x, _ := json.Marshal(info)
		err = errors.New(fmt.Sprint("Cannot find mapred node", x))
		log.Error(err)
		return
	}

	subIndex := info.SubIndex

	// Inputs Files
	inputFiles := fNode.GetInputFiles().GetFiles()
	readers := make([]records.RecordReader, 0)
	for fidx := int(subIndex) * fNode.GetBatchSize(); fidx < len(inputFiles) && fidx < int(subIndex+1) * fNode.GetBatchSize(); fidx++ {
		file := inputFiles[fidx]
		log.Debug("Opening mapper input file", file)
		reader, err := e.getBucket(fNode.GetInputFiles()).OpenRead(file)
		if err != nil {
			log.Errorf("Fail to open object %s: %v", file, err)
			return err
		}
		recordReader := records.MakeRecordReader(fNode.GetInputFiles().GetType(), map[string]interface{}{"reader": reader})
		readers = append(readers, recordReader)
	}
	batchReader := records.NewChainReader(readers)

	// Intermediate writers
	outputFile := fNode.GetOutputFiles().GetFiles()[int(subIndex)]

	if err := e.getBucket(fNode.GetOutputFiles()).CreateDir([]string{outputFile}); err != nil {
		log.Errorf("cannot create dir err: %v", err)
		return err
	}
	writer, err := e.getBucket(fNode.GetOutputFiles()).OpenWrite(outputFile)

	if err != nil {
		log.Errorf("Failed to open intermediate: %v", err)
		return err
	}

	e.doFilter(batchReader, writer, e.getWorker().workerID, fNode.GetFilter())

	var err1, err2 error
	//master should delete intermediate files
	for _, reader := range readers {
		err1 = reader.Close()
		if err1 != nil {
			log.Error(err1)
		}
	}
	err2 = writer.Close()

	if err1 != nil || err2 != nil {
		return errors.New(fmt.Sprint(err1, "\n", err2))
	}

	return err
}

func (e *FilterExecutor) doFilter(rr records.RecordReader, writer io.Writer, workerID int64, filter mapred.Filter) (err error) {
	startTime := time.Now()

	// map
	waitc := make(chan struct{})
	inputKV := make(chan *kmrpb.KV, 1024)
	filter.Init()
	keyClass, valueClass := filter.GetInputKeyTypeConverter(), filter.GetInputValueTypeConverter()
	go func() {
		for kvpair := range inputKV {
			key := keyClass.FromBytes(kvpair.Key)
			value := valueClass.FromBytes(kvpair.Value)
			if err := filter.Filter(key, value, writer); err != nil {
				log.Fatal("Do filter error", err)
			}
		}
		close(waitc)
	}()
	for rr.HasNext() {
		inputKV <- RecordToKV(rr.Pop())
	}
	close(inputKV)
	<-waitc

	filter.After()

	log.Debug("DONE Filter. Took:", time.Since(startTime))
	return
}
