package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/master"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util/log"

	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/rand"
)

// XXX this should be unified with master.map/reducePhase
const (
	mapPhase    = "map"
	reducePhase = "reduce"
)

type Worker struct {
	job                                               *jobgraph.Job
	mapBucket, interBucket, reduceBucket, flushBucket bucket.Bucket
	workerID                                          int64
	flushOutSize                                      int
	masterAddr                                        string
	hostName                                          string
}

// NewWorker create a worker
func NewWorker(job *jobgraph.Job, workerID int64, masterAddr string, flushOutSize int, mapBucket, interBucket, reduceBucket, flushBucket bucket.Bucket) *Worker {
	worker := Worker{
		job:          job,
		masterAddr:   masterAddr,
		mapBucket:    mapBucket,
		interBucket:  interBucket,
		reduceBucket: reduceBucket,
		flushBucket:  flushBucket,
		workerID:     workerID,
		flushOutSize: flushOutSize,
	}
	if hn, err := os.Hostname(); err == nil {
		worker.hostName = hn
	}
	return &worker
}

func (w *Worker) getBucket(files jobgraph.Files) bucket.Bucket {
	switch files.GetBucketType() {
	case jobgraph.MapBucket:
		return w.mapBucket
	case jobgraph.ReduceBucket:
		return w.reduceBucket
	case jobgraph.InterBucket:
		return w.interBucket
	}
	return nil
}

func (w *Worker) Run() {
	//Here we must know the master address
	var retcode kmrpb.ReportInfo_ErrorCode

	var cc *grpc.ClientConn
	for {
		var err error
		cc, err = grpc.Dial(w.masterAddr, grpc.WithInsecure())
		if err != nil {
			log.Error("cannot connect to master", err)
			time.Sleep(time.Duration(20 * time.Second))
		} else {
			break
		}
	}

	masterClient := kmrpb.NewMasterClient(cc)
	for {
		log.Info(w.hostName, "is requesting task...")
		task, err := masterClient.RequestTask(context.Background(), &kmrpb.RegisterParams{
			JobName:    w.job.GetName(),
			WorkerID:   w.workerID,
			WorkerName: w.hostName,
		})

		if err != nil || task.Retcode != 0 {
			log.Error(err)
			time.Sleep(time.Duration(rand.IntnRange(1, 3)) * time.Second)
			continue
		}

		log.Info(w.hostName, "get a task", task)

		taskInfo := task.Taskinfo
		timer := time.NewTicker(master.HeartBeatTimeout / 9)
		// Prevent memory leak. Stop a ticker will not close the channel
		timerStopped := make(chan bool, 1)
		lastHeartbeatSent := make(chan bool, 1)
		go func() {
			for {
				select {
				case <-timer.C:
					// SendHeartBeat
					_, err := masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
						TaskInfo: taskInfo,
						WorkerID: w.workerID,
						Retcode:  kmrpb.ReportInfo_DOING,
					})
					if err != nil {
						log.Error("Failed to send heartbeat message", err)
					}
				case <-timerStopped:
					lastHeartbeatSent <- true
					close(lastHeartbeatSent)
					return
				}
			}
		}()

		err = w.executeTask(taskInfo)

		retcode = kmrpb.ReportInfo_FINISH
		if err != nil {
			log.Debug(err)
			retcode = kmrpb.ReportInfo_ERROR
		}
		timer.Stop()
		timerStopped <- true

		// synchronize with heartbeat goroutine
		<-lastHeartbeatSent

		for {
			log.Info("Reporting job result, code", retcode, "job", taskInfo)
			ret, err := masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
				TaskInfo: taskInfo,
				WorkerID: w.workerID,
				Retcode:  retcode,
			})
			log.Debug("Reporting job master responsd:", err, ret)
			// backoff
			if err != nil || ret.Retcode != 0 {
				// make sure this message received by master
				// so when we are considered to be timeout by master
				// we can make master restore us back
				log.Error("Failed to send task state message", err)
				time.Sleep(time.Duration(rand.IntnRange(1, 3)) * time.Second)
			} else {
				break
			}
		}
	}
}

func (w *Worker) executeTask(task *kmrpb.TaskInfo) (err error) {
	cw := &ComputeWrapClass{}
	err = nil
	mapredNode := w.job.GetMapReduceNode(task.JobNodeName, int(task.MapredNodeIndex))
	if mapredNode == nil {
		x, _ := json.Marshal(task)
		err = errors.New(fmt.Sprint("Cannot find mapred node", x))
		log.Error(err)
		return
	}
	cw.BindMapper(mapredNode.GetMapper())
	cw.BindReducer(mapredNode.GetReducer())
	cw.BindCombiner(mapredNode.GetCombiner())
	switch task.Phase {
	case mapPhase:
		err = w.runMapper(cw, mapredNode, task.SubIndex)
	case reducePhase:
		err = w.runReducer(cw, mapredNode, task.SubIndex)
	default:
		x, _ := json.Marshal(task)
		err = errors.New(fmt.Sprint("Unkown task phase", x))
	}
	return err
}

func (w *Worker) runReducer(cw *ComputeWrapClass, node *jobgraph.MapReduceNode, subIndex int32) error {
	readers := make([]records.RecordReader, 0)
	interFiles := node.GetInterFileNameGenerator().GetReducerInputFiles(int(subIndex))
	for _, interFile := range interFiles {
		//TODO: How to deal with backup Task ?
		reader, err := w.interBucket.OpenRead(interFile)
		recordReader := records.NewStreamRecordReader(reader)
		if err != nil {
			log.Errorf("Failed to open intermediate: %v", err)
		} else {
			readers = append(readers, recordReader)
		}
	}

	outputFile := node.GetOutputFiles().GetFiles()[subIndex]
	writer, err := w.getBucket(node.GetOutputFiles()).OpenWrite(outputFile)
	if err != nil {
		log.Errorf("Failed to open reduce output file: %v", err)
		return err
	}
	recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
	if err := cw.DoReduce(readers, recordWriter); err != nil {
		log.Errorf("Fail to Reduce: %v", err)
		return err
	}
	err = recordWriter.Close()
	return err
}

func (w *Worker) runMapper(cw *ComputeWrapClass, node *jobgraph.MapReduceNode, subIndex int32) error {
	// Inputs Files
	inputFiles := node.GetInputFiles().GetFiles()
	readers := make([]records.RecordReader, 0)
	for fidx := int(subIndex) * node.GetMapperBatchSize(); fidx < len(inputFiles) && fidx < int(subIndex+1)*node.GetMapperBatchSize(); fidx++ {
		file := inputFiles[fidx]
		log.Debug("Opening mapper input file", file)
		reader, err := w.getBucket(node.GetInputFiles()).OpenRead(file)
		if err != nil {
			log.Errorf("Fail to open object %s: %v", file, err)
			return err
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

	if err := w.interBucket.CreateDir(interFiles); err != nil {
		log.Errorf("cannot create dir err: %v", err)
		return err
	}
	writers := make([]records.RecordWriter, 0)
	for i := 0; i < node.GetReducerNum(); i++ {
		intermediateFileName := interFiles[i]
		writer, err := w.interBucket.OpenWrite(intermediateFileName)
		recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
		if err != nil {
			log.Errorf("Failed to open intermediate: %v", err)
			return err
		}
		writers = append(writers, recordWriter)
	}

	cw.DoMap(batchReader, writers, w.flushBucket, w.flushOutSize, node.GetIndex(), node.GetReducerNum(), w.workerID)

	var err1, err2 error
	//master should delete intermediate files
	for _, reader := range readers {
		err1 = reader.Close()
		if err1 != nil {
			log.Error(err1)
		}
	}
	for _, writer := range writers {
		err2 := writer.Close()
		if err2 != nil {
			log.Error(err2)
		}
	}

	if err1 != nil || err2 != nil {
		return errors.New(fmt.Sprint(err1, "\n", err2))
	}

	return nil
}
