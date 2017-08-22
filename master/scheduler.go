package master

import (
	"errors"
	"fmt"
	"sync"

	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/util/log"
)

type TaskDescription struct {
	ID                 int
	JobNodeName        string
	MapReduceNodeIndex int32
	Phase              string
	PhaseSubIndex      int
}

type ReportFunction func(task TaskDescription, state int)
type RequestFunction func() (TaskDescription, error)
type PushJobFunction func(jobDesc jobgraph.JobDescription) (state <-chan int)

type EventHandler interface {
	TaskSucceeded(jobDesc TaskDescription) error
	TaskFailed(jobDesc TaskDescription) error
	MapReduceNodeSucceed(node *jobgraph.MapReduceNode) error
	MapReduceNodeFailed(node *jobgraph.MapReduceNode) error
	JobNodeSucceeded(node *jobgraph.JobNode) error
	JobNodeFailed(node *jobgraph.JobNode) error
	JobFinished(job *jobgraph.Job)
}

const (
	StateIdle = iota
	StateInProgress
	StateCompleted

	ResultOK
	ResultFailed

	mapPhase    = "map"
	reducePhase = "reduce"
)

var NoAvailableJobError = errors.New("No available job")
var DuplicateInitializeError = errors.New("Duplicated initialization")

// Scheduler responsible to schedule tasks.
// every MapReduce node can be divided into (nMappers*nReducer) tasks.
// executor request a task to execute.
type Scheduler struct {
	jobGraph *jobgraph.Job

	jobResultChanMap map[*mapReduceJob]chan<- int
	availableJobs    []*mapReduceJob
	taskDescID       int

	mapperFinishedCnt  map[*mapReduceJob]int
	reducerFinishedCnt map[*mapReduceJob]int
	phaseMap           map[*mapReduceJob]string
	taskStateMap       map[*task]int
	taskIDMap          map[int]*task

	requestFunc RequestFunction
	reportFunc  ReportFunction
}

type task struct {
	phase     string
	taskIndex int

	job *mapReduceJob
}

type mapReduceJob struct {
	//Use this can get a unique mapredNode
	jobDesc               jobgraph.JobDescription
	mapTasks, reduceTasks []*task
}

// createMapReduceTasks divide a MapReduceNode represented by a jobgraph.JobDescription into smallest executable task
func (s *Scheduler) createMapReduceTasks(desc jobgraph.JobDescription) (mrJob *mapReduceJob, err error) {
	mrJob = &mapReduceJob{}
	mrNode := s.jobGraph.GetMapReduceNode(desc.JobNodeName, int(desc.MapReduceNodeIndex))
	if mrNode == nil {
		err = errors.New(fmt.Sprint("Cannot get mapReduceNode from job graph, job description is", desc))
		return
	}
	mrJob.jobDesc = desc
	fillTask := func(job *mapReduceJob, phase string) {
		var nTasks int
		var batchSize int
		jobDesc := job.jobDesc
		switch phase {
		case mapPhase:
			batchSize = jobDesc.MapperBatchSize
			nTasks = (jobDesc.MapperObjectSize + batchSize - 1) / batchSize
		case reducePhase:
			batchSize = 1
			nTasks = jobDesc.ReducerNumber
		}

		tasks := make([]*task, nTasks)
		for i := 0; i < nTasks; i++ {
			tasks[i] = &task{
				phase:     phase,
				job:       job,
				taskIndex: i,
			}
		}

		switch phase {
		case mapPhase:
			job.mapTasks = tasks
		case reducePhase:
			job.reduceTasks = tasks
		default:
			//XXX: should not be here
			log.Fatal("Unknown phase")
		}
	}
	fillTask(mrJob, mapPhase)
	fillTask(mrJob, reducePhase)
	return
}

// RequestTask request a task to execute
func (s *Scheduler) RequestTask() (TaskDescription, error) {
	return s.requestFunc()
}

// ReportTask report the execution result of a task
func (s *Scheduler) ReportTask(task TaskDescription, state int) {
	s.reportFunc(task, state)
}

// StartSchedule After start scheduling, RequestTask and ReportTask will be available
// schedule granularity is task(a concrete map or reduce to be run)
func (s *Scheduler) StartSchedule(visitor EventHandler) error {
	if s.requestFunc != nil || s.reportFunc != nil {
		return DuplicateInitializeError
	}
	s.jobResultChanMap = make(map[*mapReduceJob]chan<- int)
	s.availableJobs = make([]*mapReduceJob, 0)
	s.taskDescID = 0
	s.mapperFinishedCnt = make(map[*mapReduceJob]int)
	s.reducerFinishedCnt = make(map[*mapReduceJob]int)
	s.phaseMap = make(map[*mapReduceJob]string)
	s.taskStateMap = make(map[*task]int)
	s.taskIDMap = make(map[int]*task)

	type reportJobInput struct {
		desc   TaskDescription
		result int
	}
	type requestJobOutput struct {
		desc TaskDescription
		err  error
	}
	type pushJobInput struct {
		desc   jobgraph.JobDescription
		output chan (<-chan int)
	}
	var (
		pushJobChan    = make(chan pushJobInput, 1)
		requestJobChan = make(chan (chan requestJobOutput), 1)
		reportJobChan  = make(chan reportJobInput, 1)
	)

	go func() {
		for {
			select {
			case c := <-pushJobChan:
				state := make(chan int, 1)
				j, err := s.createMapReduceTasks(c.desc)
				if err != nil {
					log.Fatal(err)
				}
				s.availableJobs = append(s.availableJobs, j)
				s.jobResultChanMap[j] = state
				if _, ok := s.mapperFinishedCnt[j]; ok {
					log.Panic("Job repeated scheduled, mapperCnt != 0")
				}
				if _, ok := s.reducerFinishedCnt[j]; ok {
					log.Panic("Job repeated scheduled, reducerCnt != 0")
				}
				if _, ok := s.phaseMap[j]; ok {
					log.Panic("Job repeated scheduled, phase != nil")
				}
				s.mapperFinishedCnt[j], s.reducerFinishedCnt[j], s.phaseMap[j] = 0, 0, mapPhase
				c.output <- state
				close(c.output)
			case c := <-requestJobChan:
				out := requestJobOutput{TaskDescription{}, NoAvailableJobError}
				finished := false
				for _, processingJob := range s.availableJobs {
					var tasks *[]*task
					if s.mapperFinishedCnt[processingJob] == len(processingJob.mapTasks) {
						s.phaseMap[processingJob] = reducePhase
						tasks = &processingJob.reduceTasks
					} else if s.mapperFinishedCnt[processingJob] < len(processingJob.mapTasks) &&
						s.reducerFinishedCnt[processingJob] == 0 {
						s.phaseMap[processingJob] = mapPhase
						tasks = &processingJob.mapTasks
					} else {
						log.Fatal("After job node finished, this should not exist")
					}
					for _, task := range *tasks {
						if _, ok := s.taskStateMap[task]; !ok {
							s.taskStateMap[task] = StateIdle
						}
						if s.taskStateMap[task] == StateIdle {
							s.taskStateMap[task] = StateInProgress
							s.taskDescID++
							out = requestJobOutput{
								TaskDescription{
									ID:                 s.taskDescID,
									JobNodeName:        processingJob.jobDesc.JobNodeName,
									MapReduceNodeIndex: processingJob.jobDesc.MapReduceNodeIndex,
									Phase:              s.phaseMap[processingJob],
									PhaseSubIndex:      task.taskIndex,
								},
								nil,
							}
							s.taskIDMap[s.taskDescID] = task
							finished = true
							break
						}
					}
					if finished {
						break
					}
				}
				c <- out
				close(c)
				continue
			case rep := <-reportJobChan:
				var t *task
				var ok bool
				if t, ok = s.taskIDMap[rep.desc.ID]; !ok || t == nil {
					log.Error("Report a task doesn't exists")
					continue
				}
				if _, ok := s.taskStateMap[t]; !ok {
					log.Error("Delivered task doesn't have a state")
					continue
				}
				state := s.taskStateMap[t]
				if rep.result != ResultOK {
					s.taskStateMap[t] = StateIdle
					continue
				}
				if state != StateInProgress && state != StateCompleted {
					log.Errorf("State of task reporting finished is not processing or completed")
					continue
				}
				if state == StateInProgress {
					switch s.phaseMap[t.job] {
					case mapPhase:
						s.mapperFinishedCnt[t.job]++
					case reducePhase:
						if s.mapperFinishedCnt[t.job] != len(t.job.mapTasks) {
							log.Panic("Reducer start before mappers finished")
						}
						s.reducerFinishedCnt[t.job]++
						if s.reducerFinishedCnt[t.job] == len(t.job.reduceTasks) {
							curJobIdx := -1
							// remove this job
							for idx := range s.availableJobs {
								if s.availableJobs[idx] == t.job {
									curJobIdx = idx
								}
							}
							if curJobIdx >= 0 {
								s.availableJobs[curJobIdx] = s.availableJobs[len(s.availableJobs)-1]
								s.availableJobs = s.availableJobs[:len(s.availableJobs)-1]
								s.jobResultChanMap[t.job] <- ResultOK
								close(s.jobResultChanMap[t.job])
							} else {
								log.Error("Cannot find job which is reporting to be done")
							}
						}
					default:
						log.Panic("Phase map is not initialized")
					}
					s.taskStateMap[t] = StateCompleted
				}
			}
		}
	}()

	// PushJob Push a job to execute
	pushJobFunc := func(jobDesc jobgraph.JobDescription) <-chan int {
		input := pushJobInput{jobDesc, make(chan (<-chan int), 1)}
		pushJobChan <- input
		return <-input.output
	}

	s.requestFunc = func() (TaskDescription, error) {
		c := make(chan requestJobOutput, 1)
		requestJobChan <- c
		out := <-c
		return out.desc, out.err
	}

	s.reportFunc = func(desc TaskDescription, result int) {
		reportJobChan <- reportJobInput{desc, result}
	}

	go s.MapReduceNodeSchedule(pushJobFunc, visitor)
	return nil
}

// MapReduceNodeSchedule Schedule in granularity of MapReduceNode
func (s *Scheduler) MapReduceNodeSchedule(pushJobFunc PushJobFunction, eventHandler EventHandler) {

	waitForAll := &sync.WaitGroup{}
	jobStatusMap := make(map[*jobgraph.JobNode]int)
	mapsLock := sync.Mutex{}
	setJobNodeStatus := func(j *jobgraph.JobNode, status int) {
		mapsLock.Lock()
		defer mapsLock.Unlock()
		jobStatusMap[j] = status
	}
	statusEqualTo := func(j *jobgraph.JobNode, status int) bool {
		mapsLock.Lock()
		defer mapsLock.Unlock()
		if _, ok := jobStatusMap[j]; !ok {
			jobStatusMap[j] = StateIdle
		}
		return jobStatusMap[j] == status
	}
	// topo sort and push job to master
	var topo func(node *jobgraph.JobNode)
	topo = func(node *jobgraph.JobNode) {
		mrNodes := node.GetMapReduceNodes()
		for idx := 0; idx < len(mrNodes); {
			mrNode := mrNodes[idx]
			jobDesc := mrNode.ToJobDesc()
			if jobDesc == nil {
				log.Fatal("Failed to convert node ", jobDesc)
			}
			if result := <-pushJobFunc(*jobDesc); result == ResultOK {
				err := eventHandler.MapReduceNodeSucceed(mrNode)
				if err == nil {
					idx++
				} else {
					log.Error(err)
				}
			} else {
				err := eventHandler.MapReduceNodeFailed(mrNode)
				log.Error("Map reduce node failed", err, *mrNode)
			}
		}
		setJobNodeStatus(node, StateCompleted)
		for _, nextJobNode := range node.GetDependencyOf() {
			allDepCompleted := true
			for _, dep := range nextJobNode.GetDependencies() {
				if !statusEqualTo(dep, StateCompleted) {
					allDepCompleted = false
				}
			}
			if !allDepCompleted {
				continue
			}
			if !statusEqualTo(nextJobNode, StateIdle) {
				continue
			}
			setJobNodeStatus(nextJobNode, StateInProgress)
			waitForAll.Add(1)
			go topo(nextJobNode)
		}
		waitForAll.Done()
	}

	for _, n := range s.jobGraph.GetRootNodes() {
		if len(n.GetDependencies()) != 0 {
			continue
		}
		setJobNodeStatus(n, StateInProgress)
		waitForAll.Add(1)
		go topo(n)
	}
	waitForAll.Wait()
	eventHandler.JobFinished(s.jobGraph)
}
