package master

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/jobgraph"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/util/log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	HeartBeatCodePulse = iota
	HeartBeatCodeDead
	HeartBeatCodeFinished

	HeartBeatTimeout = 90 * time.Second
)

type heartBeatInput struct {
	heartBeatCode int
	task          TaskDescription
}

// Master is a map-reduce controller. It stores the state for each task and other runtime progress statuses.
type Master struct {
	sync.Mutex
	port string // Master listening port, like ":50051"

	heartbeat     map[int64]chan heartBeatInput // Heartbeat channel for each worker
	workerTaskMap map[int64]TaskDescription
	workerNameMap map[int64]string

	job       *jobgraph.Job
	scheduler Scheduler

	mapBucket, interBucket, reduceBucket bucket.Bucket
	waitFinish                           sync.WaitGroup

	maxFailureTime int

	ck *CheckPoint
}

func (m *Master) TaskSucceeded(t TaskDescription) error {
	if m.ck != nil {
		m.ck.MarkTaskSucceeded(t)
	}
	return nil
}

func (m *Master) TaskFailed(t TaskDescription) error {
	if m.ck != nil {
		state := m.ck.GetTaskState(t)
		log.Info("Task", t,"has failed for", state.failureTime, "times", "limit: ", m.maxFailureTime)
		m.ck.IncreaseTaskFailureTime(t)
	}
	return nil
}

func (m *Master) getBucket(files jobgraph.Files) bucket.Bucket {
	switch files.GetBucketType() {
	case jobgraph.MapBucket:
		return m.mapBucket
	case jobgraph.ReduceBucket:
		return m.reduceBucket
	case jobgraph.InterBucket:
		return m.interBucket
	}
	return nil
}

func (m *Master) TaskNodeSucceed(node jobgraph.TaskNode) error {
	mrNode, ok := node.(*jobgraph.MapReduceNode)
	if ok {
		// Delete inter files
		for mapperIdx := 0; mapperIdx < mrNode.GetMapperNum(); mapperIdx++ {
			for reducerIdx := 0; reducerIdx < mrNode.GetReducerNum(); reducerIdx++ {
				m.interBucket.Delete(mrNode.GetInterFileNameGenerator().GetFile(mapperIdx, reducerIdx))
			}
		}
	}
	// Delete previous node output files
	if p := node.GetPrev(); p != nil {
		for _, file := range p.GetOutputFiles().GetFiles() {
			m.getBucket(p.GetOutputFiles()).Delete(file)
		}
	}
	return nil
}

func (m *Master) TaskNodeFailed(node jobgraph.TaskNode) error {
	return nil
}

func (m *Master) JobNodeSucceeded(node *jobgraph.JobNode) error {
	return nil
}

func (m *Master) JobNodeFailed(node *jobgraph.JobNode) error {
	return nil
}

func (m *Master) JobFinished(job *jobgraph.Job) {
	m.waitFinish.Done()
}

func (m *Master) resetWorker(workerID int64) {
	m.Lock()
	defer m.Unlock()
Loop:
	for {
		select {
		case <-m.heartbeat[workerID]:
			continue
		default:
			break Loop
		}
	}
	delete(m.workerTaskMap, workerID)
}

// CheckHeartbeatForEachWorker
// CheckHeartbeat keeps checking the heartbeat of each worker. It is either DEAD, PULSE, FINISH or losing signal of
// heartbeat.
// If the task is DEAD (occur error while the worker is doing the task) or cannot detect heartbeat in time. Master
// will releases the task, so that another work can takeover
// Master will check the heartbeat every 5 seconds. If m cannot detect any heartbeat in the meantime, master
// regards it as a DEAD worker.
func (m *Master) CheckHeartbeatForEachWorker(workerID int64, heartbeat chan heartBeatInput) {
	for {
		timeout := time.After(HeartBeatTimeout)
		select {
		case <-timeout:
			// the worker fuck up, release the task
			log.Error("Worker: ", workerID, "fuck up")
			m.scheduler.ReportTask(m.workerTaskMap[workerID], ResultFailed)
			// when timeout happens, nobody not do it for us
			m.resetWorker(workerID)
			return
		case hb := <-heartbeat:
			// the worker is doing his job
			switch hb.heartBeatCode {
			case HeartBeatCodeDead:
				log.Error("Worker: ", workerID, "report task error", hb.task)
				m.scheduler.ReportTask(hb.task, ResultFailed)
				return
			case HeartBeatCodeFinished:
				log.Error("Worker: ", workerID, "finish task", hb.task)
				m.scheduler.ReportTask(hb.task, ResultOK)
				return
			case HeartBeatCodePulse:
				continue
			}
		}
	}
}

// Run run and wait until all job finished
func (m *Master) Run() {
	m.scheduler.StartSchedule(m)

	m.waitFinish.Add(1)
	go func() {
		lis, err := net.Listen("tcp", ":"+m.port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Infof("listen localhost: %s", m.port)
		s := grpc.NewServer()
		kmrpb.RegisterMasterServer(s, &server{master: m})
		reflection.Register(s)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	m.waitFinish.Wait()
}

type server struct {
	sync.Mutex
	master *Master
}

// RequestTask is to deliver a task to worker.
func (s *server) RequestTask(ctx context.Context, in *kmrpb.RegisterParams) (*kmrpb.Task, error) {
	s.master.Lock()
	defer s.master.Unlock()
	var t TaskDescription
	var err error

	if t, ok := s.master.workerTaskMap[in.WorkerID]; ok {
		log.Errorf("WorkerID %v is working on %v, waiting for timeout", in.WorkerID, t)
		// this should never happen
		return &kmrpb.Task{Retcode: -1}, errors.New("worker is working on another job and have not reported result")
	}

	for {
		t, err = s.master.scheduler.RequestTask()
		if err != nil {
			return &kmrpb.Task{
				Retcode: -1,
			}, err
		}

		if s.master.ck != nil {
			state := s.master.ck.GetTaskState(t)
			if state.succeeded || (s.master.maxFailureTime > 0 && state.failureTime > s.master.maxFailureTime) {
				if state.succeeded {
					log.Info("Job", t, "had been finished according to checkpoint")
				} else {
					log.Info("Job", t, "is finished because it has failed for more than",
						s.master.maxFailureTime, "times")
				}
				go func(taskDesc TaskDescription) {
					s.master.scheduler.ReportTask(taskDesc, ResultOK)
				}(t)
			} else {
				break
			}
		} else {
			break
		}
	}

	s.master.workerNameMap[in.WorkerID] = in.WorkerName
	if _, ok := s.master.heartbeat[in.WorkerID]; !ok {
		s.master.heartbeat[in.WorkerID] = make(chan heartBeatInput, 10)
	}
	s.master.workerTaskMap[in.WorkerID] = t
	go s.master.CheckHeartbeatForEachWorker(in.WorkerID, s.master.heartbeat[in.WorkerID])
	log.Infof("deliver a task Jobname: %v MapredNodeID: %v Phase: %v PhaseSubIndex: %v to %v:%v",
		t.JobNodeName, t.TaskNodeIndex, t.Phase, t.PhaseSubIndex, in.WorkerID, in.WorkerName)
	return &kmrpb.Task{
		Retcode: 0,
		Taskinfo: &kmrpb.TaskInfo{
			JobNodeName:     t.JobNodeName,
			MapredNodeIndex: int32(t.TaskNodeIndex),
			Phase:           int32(t.Phase),
			SubIndex:        int32(t.PhaseSubIndex),
		},
	}, err
}

// ReportTask is for executor to report its progress state to master.
func (s *server) ReportTask(ctx context.Context, in *kmrpb.ReportInfo) (*kmrpb.Response, error) {
	s.master.Lock()
	defer s.master.Unlock()
	var t TaskDescription
	var ok bool
	if t, ok = s.master.workerTaskMap[in.WorkerID]; !ok {
		log.Errorf("WorkerID %v is not working on anything or has been timeout", in.WorkerID)
		// this timeout worker is still alive and will not send any heartbeat on this job never
		if in.Retcode != kmrpb.ReportInfo_DOING {
			log.Errorf("WorkerID %v had been timeout but now is alive", in.WorkerID)
		}
		return &kmrpb.Response{Retcode: 0}, nil
	}

	var heartbeatCode int
	switch in.Retcode {
	case kmrpb.ReportInfo_FINISH:
		delete(s.master.workerTaskMap, in.WorkerID)
		heartbeatCode = HeartBeatCodeFinished
	case kmrpb.ReportInfo_DOING:
		heartbeatCode = HeartBeatCodePulse
	case kmrpb.ReportInfo_ERROR:
		delete(s.master.workerTaskMap, in.WorkerID)
		heartbeatCode = HeartBeatCodeDead
	default:
		panic("unknown ReportInfo")
	}
	go func(ch chan<- heartBeatInput) {
		ch <- heartBeatInput{heartbeatCode, t}
	}(s.master.heartbeat[in.WorkerID])

	return &kmrpb.Response{Retcode: 0}, nil
}

// NewMaster Create a master, waiting for workers
func NewMaster(job *jobgraph.Job, port string, mapBucket, interBucket, reduceBucket bucket.Bucket, ck *CheckPoint, maxFailure int) *Master {
	m := &Master{
		port:           port,
		workerTaskMap:  make(map[int64]TaskDescription),
		heartbeat:      make(map[int64]chan heartBeatInput),
		workerNameMap:  make(map[int64]string),
		job:            job,
		mapBucket:      mapBucket,
		interBucket:    interBucket,
		reduceBucket:   reduceBucket,
		ck:             ck,
		maxFailureTime: maxFailure,
	}

	m.scheduler = Scheduler{
		jobGraph: job,
	}
	return m
}
