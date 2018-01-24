package master

import (
	"errors"
	"fmt"
	"sync"

	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/util/log"
)

type TaskDescription struct {
	ID            int
	JobNodeName   string
	TaskNodeIndex int
	Phase         int
	PhaseSubIndex int
}

type ReportFunction func(task TaskDescription, state int)
type RequestFunction func() (TaskDescription, error)
type PushJobFunction func(jobDesc jobgraph.JobDescription) (state <-chan int)

type EventHandler interface {
	TaskSucceeded(jobDesc TaskDescription) error
	TaskFailed(jobDesc TaskDescription) error
	TaskNodeSucceed(node jobgraph.TaskNode) error
	TaskNodeFailed(node jobgraph.TaskNode) error
	JobNodeSucceeded(node *jobgraph.JobNode) error
	JobNodeFailed(node *jobgraph.JobNode) error
	JobFinished(job *jobgraph.Job)
}

const (
	StateIdle       = iota
	StateInProgress
	StateCompleted

	ResultOK
	ResultFailed
)

var NoAvailableJobError = errors.New("no available job")
var DuplicateInitializeError = errors.New("duplicated initialization")

// Scheduler responsible to schedule tasks.
// every MapReduce node can be divided into (nMappers*nReducer) tasks.
// executor request a task to execute.
type Scheduler struct {
	jobGraph *jobgraph.Job

	jobResultChanMap map[*TaskNodeJob]chan<- int
	availableJobs    []*TaskNodeJob
	taskDescID       int

	// how many task in TaskNode's phase x has been finished
	phaseFinishedCnt map[*TaskNodeJob]map[int]int
	phaseMap         map[*TaskNodeJob]int
	taskStateMap     map[*task]int
	taskIDMap        map[int]*task

	requestFunc RequestFunction
	reportFunc  ReportFunction
}

type task struct {
	phase          int
	phaseTaskIndex int

	job *TaskNodeJob
}

type TaskNodeJob struct {
	// Use this can get a unique TaskNode
	taskNode   jobgraph.TaskNode
	phaseTasks [][]*task
}

// createMapReduceTasks divide a TaskNode represented by a jobgraph.JobDescription into smallest executable task
func (s *Scheduler) createMapReduceTasks(desc jobgraph.JobDescription) (tnJob *TaskNodeJob, err error) {
	tnJob = &TaskNodeJob{}
	taskNode := s.jobGraph.GetTaskNode(desc.JobNodeName, int(desc.TaskNodeIndex))
	if taskNode == nil {
		err = errors.New(fmt.Sprint("Cannot get mapReduceNode from job graph, job description is", desc))
		return
	}
	tnJob.taskNode = taskNode
	for i := 0; i < taskNode.GetPhaseCount(); i++ {
		ts := make([]*task, taskNode.GetTaskCountOfPhase(i))
		for idx := range ts {
			ts[idx] = &task{
				phase:          i,
				job:            tnJob,
				phaseTaskIndex: idx,
			}
		}
		tnJob.phaseTasks = append(tnJob.phaseTasks, ts)
	}
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
	s.jobResultChanMap = make(map[*TaskNodeJob]chan<- int)
	s.availableJobs = make([]*TaskNodeJob, 0)
	s.taskDescID = 0

	s.phaseFinishedCnt = make(map[*TaskNodeJob]map[int]int)
	s.phaseMap = make(map[*TaskNodeJob]int)
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
				if _, ok := s.phaseFinishedCnt[j]; ok {
					log.Panic("Job repeated scheduled, phase finished map != nil")
				}
				if _, ok := s.phaseMap[j]; ok {
					log.Panic("Job repeated scheduled, phase != nil")
				}
				// create phase finish count map for this TaskNode job
				s.phaseFinishedCnt[j] = make(map[int]int)
				// start from phase 0
				s.phaseMap[j] = 0

				c.output <- state
				close(c.output)
			case c := <-requestJobChan:
				out := requestJobOutput{TaskDescription{}, NoAvailableJobError}
				finished := false
				for _, processingJob := range s.availableJobs {
					taskNode := processingJob.taskNode
					jobPhase := s.phaseMap[processingJob]
					if s.phaseFinishedCnt[processingJob][jobPhase] == taskNode.GetTaskCountOfPhase(jobPhase) {
						jobPhase++
						s.phaseMap[processingJob] = jobPhase
					}
					if jobPhase >= taskNode.GetPhaseCount() {
						log.Fatal("After job node finished, this should not exist")
					}
					tasks := &processingJob.phaseTasks[jobPhase]
					for _, task := range *tasks {
						if _, ok := s.taskStateMap[task]; !ok {
							s.taskStateMap[task] = StateIdle
						}
						if s.taskStateMap[task] == StateIdle {
							s.taskStateMap[task] = StateInProgress
							s.taskDescID++
							out = requestJobOutput{
								TaskDescription{
									ID:            s.taskDescID,
									JobNodeName:   processingJob.taskNode.GetJobNode().GetName(),
									TaskNodeIndex: processingJob.taskNode.GetIndex(),
									Phase:         jobPhase,
									PhaseSubIndex: task.phaseTaskIndex,
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
					err := visitor.TaskFailed(rep.desc)
					if err != nil {
						log.Fatal(err)
					}
					continue
				}
				if state != StateInProgress && state != StateCompleted {
					log.Errorf("State of task reporting finished is not processing or completed")
					continue
				}

				if state == StateInProgress {
					jobPhase := s.phaseMap[t.job]
					// do check
					for i := 0; i < jobPhase; i++ {
						if s.phaseFinishedCnt[t.job][i] != t.job.taskNode.GetTaskCountOfPhase(i) {
							log.Fatal("Phase", i + 1, "start before mappers finished in TaskNode", t.job.taskNode.GetIndex())
						}
					}
					if s.phaseFinishedCnt[t.job][jobPhase] >= t.job.taskNode.GetTaskCountOfPhase(jobPhase) {
						log.Fatal("Phase", jobPhase, "has finished but rescheduled")
					}
					s.phaseFinishedCnt[t.job][jobPhase]++

					if s.phaseFinishedCnt[t.job][jobPhase] == t.job.taskNode.GetTaskCountOfPhase(jobPhase) {
						s.phaseMap[t.job]++
						jobPhase++
					}

					if jobPhase == t.job.taskNode.GetPhaseCount() {
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
					err := visitor.TaskSucceeded(rep.desc)
					if err != nil {
						log.Error(err)
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

	go s.taskNodeSchedule(pushJobFunc, visitor)
	return nil
}

// MapReduceNodeSchedule Schedule in granularity of TaskNode
func (s *Scheduler) taskNodeSchedule(pushJobFunc PushJobFunction, eventHandler EventHandler) {

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
		taskNodes := node.GetTaskNodes()
		for idx := 0; idx < len(taskNodes); {
			taskNode := taskNodes[idx]
			jobDesc := taskNode.ToJobDesc()
			if jobDesc == nil {
				log.Fatal("Failed to convert node ", jobDesc)
			}
			if result := <-pushJobFunc(*jobDesc); result == ResultOK {
				err := eventHandler.TaskNodeSucceed(taskNode)
				if err == nil {
					idx++
				} else {
					log.Error(err)
				}
			} else {
				err := eventHandler.TaskNodeFailed(taskNode)
				log.Error("Map reduce node failed", err, taskNode.GetIndex())
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
