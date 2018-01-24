package jobgraph

import (
	"unicode"

	"github.com/naturali/kmr/util/log"
)

type JobNode struct {
	name                       string
	startNode, endNode         TaskNode
	dependencies, dependencyOf []*JobNode
	graph                      *Job
	inputs                      Files
}

type Job struct {
	roots         []*JobNode
	taskNodes     []TaskNode
	taskNodeIndex int
	name          string
}

func (n *JobNode) DependOn(nodes ...*JobNode) *JobNode {
	n.dependencies = append(n.dependencies, nodes...)
	for _, ns := range nodes {
		ns.dependencyOf = append(ns.dependencyOf, n)
	}
	return n
}

func (n *JobNode) GetTaskNodes() (res []TaskNode) {
	res = make([]TaskNode, 0)
	for startNode := n.startNode; startNode != nil; startNode = startNode.GetNext() {
		res = append(res, startNode)
	}
	return
}

// GetDependencies return a copy of
func (n *JobNode) GetDependencies() (res []*JobNode) {
	res = make([]*JobNode, len(n.dependencies))
	copy(res, n.dependencies)
	return
}

// GetDependencyOf return a copy of
func (n *JobNode) GetDependencyOf() (res []*JobNode) {
	res = make([]*JobNode, len(n.dependencyOf))
	copy(res, n.dependencyOf)
	return
}

func (n *JobNode) GetInputFiles() Files {
	return n.startNode.GetInputFiles()
}

func (n *JobNode) GetOutputFiles() Files {
	return n.endNode.GetOutputFiles()
}

func (n *JobNode) GetName() string {
	return n.name
}

func (j *Job) AddJobNode(inputs Files, name string) *JobNode {
	jobNode := &JobNode{
		graph: j,
		name:  name,
		inputs: inputs,
	}

	return jobNode
}

// ValidateGraph validate the graph to ensure it can be excuted
func (j *Job) ValidateGraph() {
	visitedMap := make(map[*JobNode]bool)
	jobNodeNameMap := make(map[string]bool)
	mrNodeIndexMap := make(map[int]bool)
	inStackMap := make(map[int]bool)
	mapredNodeCount := 0
	var dfs func(*JobNode)
	dfs = func(node *JobNode) {
		if node.startNode == nil {
			log.Fatal("Start node in a job node should not be nil. Job:", node.name)
		}
		if _, ok := inStackMap[node.startNode.GetIndex()]; ok {
			log.Fatal("Job graph has circle around", node.name)
		}
		if _, ok := visitedMap[node]; ok {
			return
		}
		visitedMap[node] = true
		if _, ok := jobNodeNameMap[node.name]; ok {
			log.Fatal("Duplicate job name", node.name)
		}
		jobNodeNameMap[node.name] = true
		// check startNode and endNod pointer is correct
		var startFromEnd, endFromStart TaskNode
		for startFromEnd = node.endNode; startFromEnd.GetPrev() != nil; startFromEnd = startFromEnd.GetPrev() {}
		for endFromStart = node.startNode; endFromStart.GetNext() != nil; endFromStart = endFromStart.GetNext() {}
		if startFromEnd != node.startNode {
			log.Fatalf("%v walk from end to start. start does not equal to node.startNode", node.name)
		}
		if endFromStart != node.endNode {
			log.Fatalf("%v walk from start to end. end does not equal to node.endNode", node.name)
		}
		// Check whether map/reduce chain is correct
		for startNode := node.startNode; startNode != nil; startNode = startNode.GetNext() {
			mapredNodeCount++

			if _, ok := mrNodeIndexMap[startNode.GetIndex()]; ok {
				log.Fatalf("Duplicate MapReduceNode index %v in job %v", startNode.GetIndex(), node.name)
			}
			mrNodeIndexMap[startNode.GetIndex()] = true

			if len(startNode.GetInputFiles().GetFiles()) == 0 {
				log.Fatalf("%v-%v input file length is 0", node.name, startNode.GetIndex())
			}
			if len(startNode.GetOutputFiles().GetFiles()) == 0 {
				log.Fatalf("%v-v output file length is 0", node.name, startNode.GetIndex())
			}
			if startNode.GetPrev() != nil && startNode.GetInputFiles() != startNode.GetPrev().GetOutputFiles() {
				log.Fatalf("%v-%v input files doesn't equal to prev node output files", node.name, startNode.GetIndex())
			}

			startNode.Validate()
		}
		inStackMap[int(node.startNode.GetIndex())] = true
		for _, dep := range node.dependencyOf {
			dfs(dep)
		}
		delete(inStackMap, int(node.startNode.GetIndex()))
	}

	for _, n := range j.roots {
		dfs(n)
	}

	if mapredNodeCount != len(j.taskNodes) {
		log.Fatal("There is some orphan mapred node in the graph")
	}
}

func (j *Job) GetTaskNode(jobNodeName string, index int) TaskNode {
	for _, n := range j.taskNodes {
		if n.GetJobNode().name == jobNodeName && n.GetIndex() == index {
			return n
		}
	}
	return nil
}

func (j *Job) SetName(name string) {
	for _, c := range []rune(name) {
		if !unicode.IsLower(c) && c != rune('-') && !unicode.IsDigit(c) {
			log.Fatal("Job name should only contain lowercase, number and '-'")
		}
	}
	j.name = name
}

func (j *Job) GetName() string {
	return j.name
}

func (j *Job) GetRootNodes() (res []*JobNode) {
	res = make([]*JobNode, len(j.roots))
	copy(res, j.roots)
	return
}
