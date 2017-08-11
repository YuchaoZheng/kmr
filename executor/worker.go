package executor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/master"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util/log"
	"github.com/naturali/kmr/bucket"

	"google.golang.org/grpc"
)

// XXX this should be unified with master.map/reducePhase
const (
	mapPhase    = "map"
	reducePhase = "reduce"
)

type Worker struct {
	job                                  *jobgraph.Job
	mapBucket, interBucket, reduceBucket bucket.Bucket
	workerID                             int64
	flushOutSize                         int
	masterAddr                           string
}

// NewWorker create a worker
func NewWorker(job *jobgraph.Job, workerID int64, masterAddr string, flushOutSize int, mapBucket, interBucket, reduceBucket *bucket.Bucket) *Worker {
	worker := Worker{
		job: job,
		masterAddr:masterAddr,
		mapBucket:mapBucket,
		interBucket:interBucket,
		reduceBucket:reduceBucket,
		workerID: workerID,
		flushOutSize: flushOutSize,
	}
	return &worker
}

func (w *Worker) Run() {
	//Here we must know the master address
	var retcode kmrpb.ReportInfo_ErrorCode

	cc, err := grpc.Dial(w.masterAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("cannot connect to master", err)
	}
	masterClient := kmrpb.NewMasterClient(cc)
	for {
		task, err := masterClient.RequestTask(context.Background(), &kmrpb.RegisterParams{
			JobName:  w.job.GetName(),
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

func (w *Worker) executeTask(task *kmrpb.TaskInfo) {
	cw := &ComputeWrapClass{}
	mapredNode := w.job.GetMapReduceNode(task.JobNodeName, int(task.MapredNodeIndex))
	cw.BindMapper(mapredNode.GetMapper())
	cw.BindReducer(mapredNode.GetReducer())
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

func (w *Worker) runReducer(cw *ComputeWrapClass, node *jobgraph.MapReduceNode, subIndex int32) {
	readers := make([]records.RecordReader, 0)
	interFiles := node.GetInterFileNameGenerator().GetReducerInputFiles(int(subIndex))
	for _, interFile := range interFiles {
		//TODO: How to deal with backup Task ?
		reader, err := w.interBucket.OpenRead(interFile)
		recordReader := records.NewStreamRecordReader(reader)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, recordReader)
	}

	outputFile := node.GetOutputFiles().GetFiles()[subIndex]
	var bk bucket.Bucket
	if node.IsEndNode() {
		bk = w.interBucket
	} else {
		bk = w.reduceBucket
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

func (w *Worker) runMapper(cw *ComputeWrapClass, node *jobgraph.MapReduceNode, subIndex int32) {
	// Inputs Files
	inputFiles := node.GetInputFiles().GetFiles()
	readers := make([]records.RecordReader, 0)
	for fidx := int(subIndex) * node.GetMapperBatchSize(); fidx < len(inputFiles); fidx++ {
		file := inputFiles[fidx]
		reader, err := w.mapBucket.OpenRead(file)
		if err != nil {
			log.Fatalf("Fail to open object %s: %v", file, err)
		}
		recordReader := records.MakeRecordReader(node.GetInputFiles().GetType(), map[string]interface{}{"reader": reader})
		readers = append(readers, recordReader)
	}
	batchReader := records.NewChainReader(readers)

	// Intermediate writers
	interFiles := node.GetInterFileNameGenerator().GetMapperOutputFiles(int(subIndex))
	if len(interFiles) != node.GetReducerNum() {
		//XXX: this should be done in validateGraph
		log.Fatal("mapper output files count doesn't equal to reducer count")
	}

	writers := make([]records.RecordWriter, 0)
	for i := 0; i < node.GetReducerNum(); i++ {
		intermediateFileName := interFiles[i]
		writer, err := w.interBucket.OpenWrite(intermediateFileName)
		recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		writers = append(writers, recordWriter)
	}

	cw.DoMap(batchReader, writers, w.interBucket, w.flushOutSize, node.GetIndex(), node.GetReducerNum(), w.workerID)

	//master should delete intermediate files
	for _, reader := range readers {
		reader.Close()
	}
	for _, writer := range writers {
		writer.Close()
	}
}
