package executor

import (
	"context"
	"os"
	"time"
	"errors"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/master"
	"github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util/log"

	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/rand"
)

// XXX this should be unified with master.map/reducePhase
const (
	mapPhase    = "map"
	reducePhase = "reduce"
)

type ExecutorBase struct {
	w *Worker
}

func (b *ExecutorBase) setWorker(w *Worker) {
	b.w = w
}

func (b *ExecutorBase) getWorker() *Worker {
	return b.w
}

func (b *ExecutorBase) getBucket(files jobgraph.Files) bucket.Bucket {
	switch files.GetBucketType() {
	case jobgraph.MapBucket:
		return b.w.mapBucket
	case jobgraph.ReduceBucket:
		return b.w.reduceBucket
	case jobgraph.InterBucket:
		return b.w.interBucket
	}
	return nil
}

type Executor interface {
	setWorker(*Worker)
	getWorker() *Worker
	isTargetFor(jobgraph.TaskNode) bool
	handleTaskNode(kmrpb.TaskInfo, jobgraph.TaskNode) error
}

type Worker struct {
	job        *jobgraph.Job
	workerID   int64
	masterAddr string
	hostName   string
	executors  []Executor
	mapBucket,
	interBucket,
	reduceBucket,
	flushBucket bucket.Bucket
}

// NewWorker create a worker
func NewWorker(job *jobgraph.Job, workerID int64, masterAddr string, flushOutSize int, mapBucket, interBucket, reduceBucket, flushBucket bucket.Bucket) *Worker {
	w := &Worker{
		job:          job,
		masterAddr:   masterAddr,
		workerID:     workerID,
		mapBucket:    mapBucket,
		reduceBucket: reduceBucket,
		flushBucket:  flushBucket,
		interBucket:  interBucket,
	}
	mrExec := NewMapReduceExecutor(flushOutSize)
	mrExec.setWorker(w)
	fExec := NewFilterExecutor()
	fExec.setWorker(w)
	execs := make([]Executor, 0)
	execs = append(execs, mrExec, fExec)
	w.executors = execs
	if hn, err := os.Hostname(); err == nil {
		w.hostName = hn
	}
	return w
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

		var targetExecutor Executor
		taskNode := w.job.GetTaskNode(taskInfo.JobNodeName, int(taskInfo.MapredNodeIndex))
		for _, exec := range w.executors {
			if exec.isTargetFor(taskNode) {
				err = exec.handleTaskNode(*task.GetTaskinfo(), taskNode)
				targetExecutor = exec
			}
		}

		if targetExecutor == nil {
			err = errors.New("cannot find executor for this kind of task node")
		}

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
