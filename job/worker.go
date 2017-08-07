package job

import (
	"context"
	"encoding/json"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/master"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util/log"
	"google.golang.org/grpc"
)

const (
	mapPhase    = "map"
	reducePhase = "reduce"
)

type worker struct {
	jobGraph *JobGraph
	workerID int64
	flushOutSize int
	masterAddr string
}

func (w *worker) runWorker() {
	//Here we must know the master address
	var retcode kmrpb.ReportInfo_ErrorCode

	cc, err := grpc.Dial(w.masterAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("cannot connect to master", err)
	}
	masterClient := kmrpb.NewMasterClient(cc)
	for {
		task, err := masterClient.RequestTask(context.Background(), &kmrpb.RegisterParams{
			JobName:  w.jobGraph.Name,
			WorkerID: w.workerID,
		})
		if err != nil || task.Retcode != 0 {
			log.Error(err)
			// TODO: random backoff
			time.Sleep(1 * time.Second)
			continue
		}
		taskInfo := task.Taskinfo
		timer := time.NewTicker(master.HeartBeatTimeout / 2)
		go func() {
			for range timer.C {
				// SendHeartBeat
				masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
					TaskInfo: taskInfo,
					WorkerID: w.workerID,
					Retcode:  kmrpb.ReportInfo_DOING,
				})
			}
		}()

		w.executeTask(taskInfo)

		retcode = kmrpb.ReportInfo_FINISH
		if err != nil {
			log.Debug(err)
			retcode = kmrpb.ReportInfo_ERROR
		}
		timer.Stop()
		masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
			TaskInfo: taskInfo,
			WorkerID: w.workerID,
			Retcode:  retcode,
		})
		// backoff
		if err != nil {
			time.Sleep(1 * time.Second)
		}
	}
}

func (w *worker) executeTask(task *kmrpb.TaskInfo) {
	cw := &executor.ComputeWrapClass{}
	mapredNode := w.jobGraph.getMapredNode(task.JobNodeName, int(task.MapredNodeIndex))
	cw.BindMapper(mapredNode.mapper)
	cw.BindReducer(mapredNode.reducer)
	if mapredNode == nil {
		x, _ := json.Marshal(task)
		log.Error("Cannot find mapred node", x)
	}
	switch task.Phase {
	case mapPhase:
		w.runMapper(cw, mapredNode, task.SubIndex)
	case reducePhase:
		w.runReducer(cw, mapredNode, task.SubIndex)
	default:
		x, _ := json.Marshal(task)
		log.Fatal("Unknown task phase", x)
	}
}

func (w *worker) runReducer(cw *executor.ComputeWrapClass, node *mapredNode, subIndex int32) {
	readers := make([]records.RecordReader, 0)
	interFiles := node.interFiles.getReducerInputFiles(int(subIndex))
	for _, interFile := range interFiles {
		//TODO: How to deal with backup Task ?
		reader, err := w.jobGraph.interBucket.OpenRead(interFile)
		recordReader := records.NewStreamRecordReader(reader)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, recordReader)
	}

	outputFile := node.outputFiles.GetFiles()[subIndex]
	var bk bucket.Bucket
	if node.isIntermediaNode() {
		bk = w.jobGraph.interBucket
	} else {
		bk = w.jobGraph.reduceBucket
	}
	writer, err := bk.OpenWrite(outputFile)
	if err != nil {
		log.Fatalf("Failed to open intermediate: %v", err)
	}
	recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
	if err := cw.DoReduce(readers, recordWriter); err != nil {
		log.Fatalf("Fail to Reduce: %v", err)
	}
	recordWriter.Close()
}

func (w *worker) runMapper(cw *executor.ComputeWrapClass, node *mapredNode, subIndex int32) {
	// Inputs Files
	inputFiles := node.inputFiles.GetFiles()
	readers := make([]records.RecordReader, 0)
	for fidx := int(subIndex) * node.mapperBatchSize; fidx < len(inputFiles); fidx++ {
		file := inputFiles[fidx]
		reader, err := w.jobGraph.mapBucket.OpenRead(file)
		if err != nil {
			log.Fatalf("Fail to open object %s: %v", file, err)
		}
		recordReader := records.MakeRecordReader(node.inputFiles.GetType(), map[string]interface{}{"reader": reader})
		readers = append(readers, recordReader)
	}
	batchReader := records.NewChainReader(readers)

	// Intermediate writers
	interFiles := node.interFiles.getMapperOutputFiles(int(subIndex))
	if len(interFiles) != node.reducerCount {
		//XXX: this should be done in validateGraph
		log.Fatal("mapper output files count doesn't equal to reducer count")
	}

	writers := make([]records.RecordWriter, 0)
	for i := 0; i < node.reducerCount; i++ {
		intermediateFileName := interFiles[i]
		writer, err := w.jobGraph.interBucket.OpenWrite(intermediateFileName)
		recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		writers = append(writers, recordWriter)
	}

	cw.DoMap(batchReader, writers, w.jobGraph.interBucket, w.flushOutSize, node.index, node.reducerCount, w.workerID)

	//master should delete intermediate files
	for _, reader := range readers {
		reader.Close()
	}
	for _, writer := range writers {
		writer.Close()
	}
}
