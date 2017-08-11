package master

import (
	"net"
	"sync"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/util/log"
	kmrpb "github.com/naturali/kmr/pb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	HeartBeatCodePulse = iota
	HeartBeatCodeDead
	HeartBeatCodeFinished

	HeartBeatTimeout = 20 * time.Second
)

// Master is a map-reduce controller. It stores the state for each task and other runtime progress statuses.
type Master struct {
	port string // Master listening port, like ":50051"

	heartbeat     map[int64]chan int // Heartbeat channel for each worker
	workerTaskMap map[int64]TaskDescription

	job       *jobgraph.Job
	scheduler Scheduler

	mapBucket, interBucket, reduceBucket bucket.Bucket
	waitFinish                           sync.WaitGroup
}

func (m *Master) TaskSucceeded(jobDesc *jobgraph.JobDescription) error {
	return nil
}

func (m *Master) TaskFailed(jobDesc *jobgraph.JobDescription) error {
	return nil
}

func (m *Master) MapReduceNodeSucceed(node *jobgraph.MapReduceNode) error {
	// Delete inter files
	for mapperIdx := 0; mapperIdx < node.GetMapperNum(); mapperIdx++ {
		for reducerIdx := 0; reducerIdx < node.GetReducerNum(); reducerIdx++ {
			m.interBucket.Delete(node.GetInterFileNameGenerator().GetFile(mapperIdx, reducerIdx))
		}
	}
	// Delete previous node output files
	if p := node.GetPrev(); p != nil {
		for _, file := range p.GetOutputFiles().GetFiles() {
			m.reduceBucket.Delete(file)
		}
	}
	return nil
}

func (m *Master) MapReduceNodeFailed(node *jobgraph.MapReduceNode) error {
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

// CheckHeartbeatForEachWorker
// CheckHeartbeat keeps checking the heartbeat of each worker. It is either DEAD, PULSE, FINISH or losing signal of
// heartbeat.
// If the task is DEAD (occur error while the worker is doing the task) or cannot detect heartbeat in time. Master
// will releases the task, so that another work can takeover
// Master will check the heartbeat every 5 seconds. If m cannot detect any heartbeat in the meantime, master
// regards it as a DEAD worker.
func (m *Master) CheckHeartbeatForEachWorker(workerID int64, heartbeat chan int) {
	for {
		timeout := time.After(HeartBeatTimeout)
		select {
		case <-timeout:
			// the worker fuck up, release the task
			log.Error("Worker: ", workerID, "fuck up")
			m.scheduler.ReportTask(m.workerTaskMap[workerID], ResultFailed)
			delete(m.workerTaskMap, workerID)
			return
		case heartbeatCode := <-heartbeat:
			// the worker is doing his job
			switch heartbeatCode {
			case HeartBeatCodeDead:
				log.Error("Worker: ", workerID, "fuck up")
				m.scheduler.ReportTask(m.workerTaskMap[workerID], ResultFailed)
				delete(m.workerTaskMap, workerID)
				return
			case HeartBeatCodeFinished:
				m.scheduler.ReportTask(m.workerTaskMap[workerID], ResultOK)
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
	master *Master
}

// RequestTask is to deliver a task to worker.
func (s *server) RequestTask(ctx context.Context, in *kmrpb.RegisterParams) (*kmrpb.Task, error) {
	t, err := s.master.scheduler.RequestTask()
	s.master.heartbeat[in.WorkerID] = make(chan int, 10)
	s.master.workerTaskMap[in.WorkerID] = t
	go s.master.CheckHeartbeatForEachWorker(in.WorkerID, s.master.heartbeat[in.WorkerID])
	log.Infof("deliver a task Jobname: %v MapredNodeID: %v Phase: %v", t.JobNodeName, t.MapReduceNodeIndex, t.Phase)
	return &kmrpb.Task{
		Retcode: -1,
		Taskinfo: &kmrpb.TaskInfo{
			JobNodeName:     t.JobNodeName,
			MapredNodeIndex: t.MapReduceNodeIndex,
			Phase:           t.Phase,
			SubIndex:        int32(t.PhaseSubIndex),
		},
	}, err
}

// ReportTask is for executor to report its progress state to master.
func (s *server) ReportTask(ctx context.Context, in *kmrpb.ReportInfo) (*kmrpb.Response, error) {
	if _, ok := s.master.workerTaskMap[in.WorkerID]; !ok {
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

// NewMaster Create a master, waiting for workers
func NewMaster(job *jobgraph.Job, port string, mapBucket, interBucket, reduceBucket *bucket.Bucket) *Master {
	m := &Master{
		port:          port,
		workerTaskMap: make(map[int64]TaskDescription),
		heartbeat:     make(map[int64]chan int),
		job:           job,
		mapBucket:     mapBucket,
		interBucket:   interBucket,
		reduceBucket:  reduceBucket,
	}

	m.scheduler = Scheduler{
		jobGraph: job,
	}
	return m
}
