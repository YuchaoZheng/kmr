package master

import (
	"net"
	"os"
	"sync"
	"time"

	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util/log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/kubernetes"
)

const (
	mapPhase       = "map"
	reducePhase    = "reduce"

	HeartBeatCodePulse    = 0
	HeartBeatCodeDead     = 1
	HeartBeatCodeFinished = 2

	HeartBeatTimeout = 20 * time.Second

	StateIdle       = 0
	StateInProgress = 1
	StateCompleted  = 2
)

type Task struct {
	state int
	//which workers is working on it
	workers map[int64]int
	// worker use this to know what to execute exactly
	taskInfo *kmrpb.TaskInfo
	job      *ProcessingJob
}

type ProcessingJob struct {
	//Use this can get a unique mapredNode
	jobDesc *JobDescription
	//Which phase has been executing
	phase string
	//After all finished, this will be filled
	finishChan                  chan int
	mapTasks, reduceTasks       []*Task
	mapFinished, reduceFinished int
}

// Master is a map-reduce controller. It stores the state for each task and other runtime progress statuses.
type Master struct {
	sync.Mutex

	workerCtl WorkerCtl
	port      string // Master listening port, like ":50051"

	heartbeat map[int64]chan int // Heartbeat channel for each worker

	jobs       []*ProcessingJob
	nextTaskID int

	workerJobMap map[int64]*Task

	k8sclient *kubernetes.Clientset
	namespace string

	checkpointFile *os.File
}

// CheckHeartbeatForEachWorker
// CheckHeartbeat keeps checking the heartbeat of each worker. It is either DEAD, PULSE, FINISH or losing signal of
// heartbeat.
// If the task is DEAD (occur error while the worker is doing the task) or cannot detect heartbeat in time. Master
// will releases the task, so that another work can takeover
// Master will check the heartbeat every 5 seconds. If master cannot detect any heartbeat in the meantime, master
// regards it as a DEAD worker.
func (master *Master) CheckHeartbeatForEachWorker(task *Task, workerID int64, heartbeat chan int) {
		for {
		 timeout := time.After(HeartBeatTimeout)
		 select {
		 case <-timeout:
		 	// the worker fuck up, release the task
			log.Error("Worker: ",workerID, "fuck up")
		 	master.Lock()
		 	 if task.state == StateInProgress {
		 	 	delete(task.workers, workerID)
		 	 	if len(task.workers) == 0 {
		 	 		task.state = StateIdle
		 	 	}
		 	 }
		 	master.Unlock()
		 	return
		 case heartbeatCode := <-heartbeat:
		 	// the worker is doing his job
		 	switch heartbeatCode {
		 	case HeartBeatCodeDead:
		 		// the worker fuck up, release the task
		 		master.Lock()
		 		if task.state == StateInProgress {
		 			delete(task.workers, workerID)
		 			if len(task.workers) == 0 {
		 				task.state = StateIdle
		 			}
		 		}
		 		master.Unlock()
		 		return
		 	case HeartBeatCodeFinished:
		 		master.Lock()
				if task.state != StateInProgress && task.state != StateCompleted {
					log.Errorf("State of task reporting finished is not processing or completed")
				} else {
					if task.state == StateInProgress {
						if task.job.phase == mapPhase {
							task.job.mapFinished++
						} else {
							task.job.reduceFinished++
							if task.job.reduceFinished == len(task.job.reduceTasks) {
								curJobIdx := -1
								// remove this job
								for idx := range master.jobs {
									if master.jobs[idx] == task.job {
										curJobIdx = idx
									}
								}

								if curJobIdx >= 0 {
									master.jobs[curJobIdx] = master.jobs[len(master.jobs)-1]
									master.jobs = master.jobs[:len(master.jobs)-1]
								} else {
									log.Fatal("Cannot find job which is reporting to be done")
								}

								task.job.finishChan <- 1
							}
						}
					}
					task.state = StateCompleted
				}
				delete(master.workerJobMap, workerID)

		 		master.Unlock()
		 		return
		 	case HeartBeatCodePulse:
		 		continue
		 	}
		 }
		}
}

// Schedule pipes into tasks for the phase (map or reduce). It will return after all the tasks are finished.
func (master *Master) fillProcessingJob(phase string, pj *ProcessingJob) {
	var nTasks int
	var batchSize int
	jobDesc := pj.jobDesc
	switch phase {
	case mapPhase:
		batchSize = jobDesc.MapperBatchSize
		nTasks = (jobDesc.MapperObjectSize + batchSize - 1) / batchSize
	case reducePhase:
		nTasks = jobDesc.ReducerNumber
		batchSize = 1
	}

	tasks := make([]*Task, nTasks)
	for i := 0; i < nTasks; i++ {
		taskInfo := &kmrpb.TaskInfo{
			JobNodeName:     pj.jobDesc.JobNodeName,
			MapredNodeIndex: pj.jobDesc.MapredNodeIndex,
			Phase:           phase,
			SubIndex:        int32(i),
		}

		tasks[i] = &Task{
			state:    StateIdle,
			workers:  make(map[int64]int),
			taskInfo: taskInfo,
			job:      pj,
		}
	}

	if phase == mapPhase {
		pj.mapTasks = tasks
	} else if phase == reducePhase {
		pj.reduceTasks = tasks
	} else {
		//XXX: should not be here
		panic("Unknown phase")
	}
}

type server struct {
	master *Master
}

// RequestTask is to deliver a task to worker.
func (s *server) RequestTask(ctx context.Context, in *kmrpb.RegisterParams) (*kmrpb.Task, error) {
	log.Infof("register %s", in.JobName)
	s.master.Lock()
	defer s.master.Unlock()

	for _, processingJob := range s.master.jobs {
		if processingJob.mapFinished == len(processingJob.mapTasks) {
			processingJob.phase = reducePhase
		}
		var tasks *[]*Task
		if processingJob.phase == mapPhase {
			tasks = &processingJob.mapTasks
		} else {
			tasks = &processingJob.reduceTasks
		}
		for _, task := range *tasks {
			if task.state == StateIdle {
				task.state = StateInProgress
				//TODO: Check worker for this task is alive
				if _, ok := s.master.heartbeat[in.WorkerID]; !ok {
					s.master.heartbeat[in.WorkerID] = make(chan int, 8)
				}
				go s.master.CheckHeartbeatForEachWorker(task, in.WorkerID, s.master.heartbeat[in.WorkerID])
				log.Infof("deliver a task Jobname: %v MapredNodeID: %v Phase: %v", processingJob.jobDesc.JobNodeName, processingJob.jobDesc.MapredNodeIndex, processingJob.phase)
				task.workers[in.GetWorkerID()] = 1
				s.master.workerJobMap[in.GetWorkerID()] = task
				return &kmrpb.Task{
					Retcode:  0,
					Taskinfo: task.taskInfo,
				}, nil
			}
		}
	}
	log.Debug("no task right now")
	return &kmrpb.Task{
		Retcode: -1,
	}, nil
}

// ReportTask is for executor to report its progress state to master.
func (s *server) ReportTask(ctx context.Context, in *kmrpb.ReportInfo) (*kmrpb.Response, error) {
	log.Debugf("get heartbeat Phase=%v, TaskID=%v, WorkerID=%v", in.GetTaskInfo().Phase, in.GetTaskInfo().JobNodeName, in.GetWorkerID())
	s.master.Lock()
	defer s.master.Unlock()

	if _, ok := s.master.workerJobMap[in.WorkerID]; !ok {
		log.Errorf("WorkerID %v is not working on anything", in.WorkerID)
		return &kmrpb.Response{Retcode: 0}, nil
	}

	var heartbeatCode int
	switch in.Retcode {
	case kmrpb.ReportInfo_FINISH:
		heartbeatCode = HeartBeatCodeFinished
	case kmrpb.ReportInfo_DOING:
		heartbeatCode = HeartBeatCodePulse
	case kmrpb.ReportInfo_ERROR:
		heartbeatCode = HeartBeatCodeDead
	default:
		panic("unknown ReportInfo")
	}
	go func(ch chan<- int) {
		ch <- heartbeatCode
	}(s.master.heartbeat[in.WorkerID])

	return &kmrpb.Response{Retcode: 0}, nil
}

func (master *Master) run() {

}

// Close Stop the master
func (master *Master) Close() {
	master.workerCtl.StopWorkers()
}

// PushJob Push a job to execute
func (master *Master) PushJob(jobDesc *JobDescription) <-chan int {
	res := make(chan int, 1)
	pj := &ProcessingJob{
		jobDesc:    jobDesc,
		phase:      mapPhase,
		finishChan: res,
	}
	master.fillProcessingJob(mapPhase, pj)
	master.fillProcessingJob(reducePhase, pj)

	master.Lock()
	defer master.Unlock()
	master.jobs = append(master.jobs, pj)

	return res
}

// NewMaster Create a master, waiting for workers
func NewMaster(port string, workerCtl WorkerCtl, namespace string, workerNum int) *Master {
	m := &Master{
		namespace: namespace,
		port:      port,
		workerCtl: workerCtl,
		workerJobMap: make(map[int64]*Task),
	}

	go func() {
		lis, err := net.Listen("tcp", ":" + port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Infof("listen localhost: %s", port)
		s := grpc.NewServer()
		kmrpb.RegisterMasterServer(s, &server{master: m})
		reflection.Register(s)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	m.heartbeat = make(map[int64]chan int)
	if workerCtl != nil {
		err := workerCtl.StartWorkers(workerNum)
		if err != nil {
			log.Fatalf("cant't start worker: %v", err)
		}
	}
	return m
}
