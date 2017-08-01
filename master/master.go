package master

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/naturali/kmr/job"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util"
	"github.com/naturali/kmr/util/log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/kubernetes"
)

const (
	mapPhase       = "map"
	reducePhase    = "reduce"
	mapreducePhase = "mr"

	HEARTBEAT_CODE_PULSE  = 0
	HEARTBEAT_CODE_DEAD   = 1
	HEARTBEAT_CODE_FINISH = 2

	HEARTBEAT_TIMEOUT = 20 * time.Second

	STATE_IDLE       = 0
	STATE_INPROGRESS = 1
	STATE_COMPLETED  = 2
)

type Task struct {
	state        int
	workers      map[int64]int
	taskInfo     *kmrpb.TaskInfo
	commitWorker int64
}

// Master is a map-reduce controller. It stores the state for each task and other runtime progress statuses.
type Master struct {
	sync.Mutex

	JobName  string             // Name of currently executing job
	JobDesc  job.JobDescription // Job description
	LocalRun bool               // Is LocalRun
	port     string             // Master listening port, like ":50051"

	wg        sync.WaitGroup     // WaitGroup for waiting all of tasks finished on each phase
	tasks     []*Task            // Holding all of tasks
	heartbeat map[int64]chan int // Heartbeat channel for each worker

	k8sclient *kubernetes.Clientset
	namespace string

	currentPhase  string
	commitMappers []int64 // commitMappers TaskId->WorkerID, indicate which worker commited the task

	checkpointFile *os.File
}

// CheckHeartbeatForEachWorker
// CheckHeartbeat keeps checking the heartbeat of each worker. It is either DEAD, PULSE, FINISH or losing signal of
// heartbeat.
// If the task is DEAD (occur error while the worker is doing the task) or cannot detect heartbeat in time. Master
// will releases the task, so that another work can takeover
// Master will check the heartbeat every 5 seconds. If master cannot detect any heartbeat in the meantime, master
// regards it as a DEAD worker.
func (master *Master) CheckHeartbeatForEachWorker(taskID int, workerID int64, heartbeat chan int) {
	for {
		timeout := time.After(HEARTBEAT_TIMEOUT)
		select {
		case <-timeout:
			// the worker fuck up, release the task
			master.Lock()
			if master.tasks[taskID].state == STATE_INPROGRESS {
				delete(master.tasks[taskID].workers, workerID)
				if len(master.tasks[taskID].workers) == 0 {
					master.tasks[taskID].state = STATE_IDLE
				}
			}
			master.Unlock()
			return
		case heartbeatCode := <-heartbeat:
			// the worker is doing his job
			switch heartbeatCode {
			case HEARTBEAT_CODE_DEAD:
				// the worker fuck up, release the task
				master.Lock()
				if master.tasks[taskID].state == STATE_INPROGRESS {
					delete(master.tasks[taskID].workers, workerID)
					if len(master.tasks[taskID].workers) == 0 {
						master.tasks[taskID].state = STATE_IDLE
					}
				}
				master.Unlock()
				return
			case HEARTBEAT_CODE_FINISH:
				master.Lock()
				if master.tasks[taskID].state != STATE_COMPLETED {
					master.tasks[taskID].state = STATE_COMPLETED
					master.tasks[taskID].commitWorker = workerID
					util.AppendCheckPoint(master.checkpointFile, master.currentPhase, taskID, workerID)
					master.wg.Done()
				}
				master.Unlock()
				return
			case HEARTBEAT_CODE_PULSE:
				continue
			}
		}
	}
}

// Schedule pipes into tasks for the phase (map or reduce). It will return after all the tasks are finished.
func (master *Master) Schedule(phase string, ck *util.MapReduceCheckPoint) {
	var nTasks int
	var batchSize int
	switch phase {
	case mapPhase:
		if master.JobDesc.Map.BatchSize == nil {
			batchSize = 1
		} else {
			batchSize = *master.JobDesc.Map.BatchSize
		}
		nTasks = len(master.JobDesc.Map.Objects) / batchSize
		if len(master.JobDesc.Map.Objects)%batchSize > 0 {
			nTasks++
		}
	case reducePhase:
		nTasks = master.JobDesc.Reduce.NReduce
		batchSize = 1
	}

	master.Lock()
	master.tasks = make([]*Task, nTasks)
	master.heartbeat = make(map[int64]chan int)
	master.currentPhase = phase
	for i := 0; i < nTasks; i++ {
		taskInfo := &kmrpb.TaskInfo{
			JobName:                master.JobName,
			Phase:                  phase,
			MapBucketJson:          master.JobDesc.MapBucket.Marshal(),
			IntermediateBucketJson: master.JobDesc.InterBucket.Marshal(),
			ReduceBucketJson:       master.JobDesc.ReduceBucket.Marshal(),
			TaskID:                 int32(i),
			NReduce:                int32(master.JobDesc.Reduce.NReduce),
			NMap:                   int32(len(master.JobDesc.Map.Objects)),
			ReaderType:             master.JobDesc.Map.ReaderType,
			MapBatchSize:           int32(batchSize),
		}
		if phase == mapPhase {
			if (i+1)*batchSize > len(master.JobDesc.Map.Objects) { // Last batch
				taskInfo.Files = master.JobDesc.Map.Objects[i*batchSize:]
			} else {
				taskInfo.Files = master.JobDesc.Map.Objects[i*batchSize : (i+1)*batchSize]
			}
		} else if phase == reducePhase {
			taskInfo.CommitMappers = master.commitMappers
		}
		master.tasks[i] = &Task{
			state:    STATE_IDLE,
			workers:  make(map[int64]int),
			taskInfo: taskInfo,
		}
	}
	master.wg.Add(nTasks)
	if ck != nil {
		switch phase {
		case mapPhase:
			for _, desc := range ck.CompletedMaps {
				master.tasks[desc.TaskID].state = STATE_COMPLETED
				master.tasks[desc.TaskID].commitWorker = desc.CommitWorker
				util.AppendCheckPoint(master.checkpointFile, phase, desc.TaskID, desc.CommitWorker)
				master.wg.Done()
			}
		case reducePhase:
			for _, desc := range ck.CompletedReduces {
				master.tasks[desc.TaskID].state = STATE_COMPLETED
				util.AppendCheckPoint(master.checkpointFile, phase, desc.TaskID, desc.CommitWorker)
				master.wg.Done()
			}
		}
	}
	master.Unlock()
	master.wg.Wait()

	if phase == mapPhase {
		master.commitMappers = make([]int64, 0)
		for _, t := range master.tasks {
			master.commitMappers = append(master.commitMappers, t.commitWorker)
		}
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

	for id, t := range s.master.tasks {
		if t.state == STATE_IDLE {
			workerID := rand.Int63()
			t.workers[workerID] = id
			t.state = STATE_INPROGRESS
			s.master.heartbeat[workerID] = make(chan int)
			log.Infof("deliver a task %d", id)
			go s.master.CheckHeartbeatForEachWorker(id, workerID, s.master.heartbeat[workerID])
			return &kmrpb.Task{
				WorkerID: workerID,
				Retcode:  0,
				Taskinfo: t.taskInfo,
			}, nil
		}
	}
	log.Debug("no task right now")
	return &kmrpb.Task{
		Retcode: -1,
	}, nil
}

// ReportTask is for executor to report its progress state to master.
func (s *server) ReportTask(ctx context.Context, in *kmrpb.ReportInfo) (*kmrpb.Response, error) {
	log.Debugf("get heartbeat phase=%s, taskid=%d, workid=%d", in.Phase, in.TaskID, in.WorkerID)
	s.master.Lock()
	defer s.master.Unlock()

	if _, ok := s.master.tasks[in.TaskID].workers[in.WorkerID]; ok {
		var heartbeatCode int
		switch in.Retcode {
		case kmrpb.ReportInfo_FINISH:
			log.Infof("task finished: phase=%s, taskid=%d, workid=%d", in.Phase, in.TaskID, in.WorkerID)
			heartbeatCode = HEARTBEAT_CODE_FINISH
		case kmrpb.ReportInfo_DOING:
			heartbeatCode = HEARTBEAT_CODE_PULSE
		case kmrpb.ReportInfo_ERROR:
			log.Infof("task error: phase=%s, taskid=%d, workid=%d", in.Phase, in.TaskID, in.WorkerID)
			heartbeatCode = HEARTBEAT_CODE_DEAD
		default:
			panic("unknown ReportInfo")
		}
		go func(ch chan<- int) {
			ch <- heartbeatCode
		}(s.master.heartbeat[in.WorkerID])
	}

	return &kmrpb.Response{Retcode: 0}, nil
}

// NewMapReduce creates a map-reduce job.
func NewMapReduce(port string, jobName string, jobDesc job.JobDescription,
	k8sclient *kubernetes.Clientset, namespace string, localRun bool, ck *util.MapReduceCheckPoint) {
	ckFile, err := os.Create(fmt.Sprintf("%s.ck", jobName))
	if err != nil {
		log.Fatalf("Can't create checkpoint file: %v", err)
	}
	defer ckFile.Close()

	master := &Master{
		JobName:        jobName,
		JobDesc:        jobDesc,
		LocalRun:       localRun,
		k8sclient:      k8sclient,
		namespace:      namespace,
		port:           port,
		checkpointFile: ckFile,
	}

	go func() {
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Infof("listen %s", port)
		s := grpc.NewServer()
		kmrpb.RegisterMasterServer(s, &server{master: master})
		reflection.Register(s)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	startTime := time.Now()
	if !master.LocalRun {
		log.Debug("Starting workers")
		err = master.startWorker("mr")
		if err != nil {
			log.Fatalf("cant't start worker: %v", err)
		}
	}

	master.Schedule(mapPhase, ck)
	log.Debug("Map DONE")
	master.Schedule(reducePhase, ck)
	log.Debug("Reduce DONE")

	if !master.LocalRun {
		err = master.killWorkers("mr")
		if err != nil {
			log.Fatalf("cant't kill worker: %v", err)
		}
	}

	log.Debug("Finish", time.Since(startTime))
}
