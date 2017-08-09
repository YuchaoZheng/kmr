package jobgraph

import (
	"unicode"

	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/util/log"
)

type mrNode struct {
	index   int
	mapper  mapred.Mapper
	reducer mapred.Reducer
	jobNode *jobNode

	mapperBatchSize         int
	reducerCount            int
	interFiles              InterFileNameGenerator
	inputFiles, outputFiles Files

	chainPrev, chainNext *mrNode
}

type jobNode struct {
	name                       string
	startNode, endNode         *mrNode
	dependencies, dependencyOf []*jobNode
	graph                      *Job
}

type Job struct {
	root          []*jobNode
	allNodes      []*mrNode
	mrNodeIndex   int
	name   string
}

func (node *mrNode) isIntermediateNode() (isIntermediateNode bool) {
	isIntermediateNode = node != node.jobNode.endNode && node.jobNode.startNode != node.jobNode.endNode
	return
}

func (node *mrNode) GetInputFiles() Files {
	return node.inputFiles
}

func (node *mrNode) GetOutputFiles() Files {
	return node.outputFiles
}

func (node *mrNode) GetInterFileNameGenerator() *InterFileNameGenerator {
	res := node.interFiles
	// prevent from writing
	return &res
}

func (node *mrNode) GetMapperNum() int {
	if node.inputFiles == nil || node.mapperBatchSize == 0{
		return 0
	}
	return (len(node.inputFiles.GetFiles()) + node.mapperBatchSize - 1) / node.mapperBatchSize
}

func (node *mrNode) GetReducerNum() int {
	return node.reducerCount
}

func (node *mrNode) GetMapper() mapred.Mapper {
	return node.mapper
}

func (node *mrNode) GetReducer() mapred.Reducer {
	return node.reducer
}

func (j *Job) ToJobDesc(node *mrNode) *JobDescription {
	return &JobDescription{
		JobNodeName:      node.jobNode.name,
		MapReduceNodeIndex:  int32(node.index),
		MapperObjectSize: len(node.inputFiles.GetFiles()),
		MapperBatchSize:  node.mapperBatchSize,
		ReducerNumber:    node.reducerCount,
	}
}


func (n *jobNode) AddMapper(mapper mapred.Mapper, inputs Files, batchSize ...int) *jobNode {
	if len(batchSize) == 0 {
		batchSize = append(batchSize, 1)
	}
	if n.endNode.reducer != nil {
		mrnode := &mrNode{
			index:           n.graph.mrNodeIndex,
			jobNode:         n,
			mapper:          mapper,
			chainPrev:       n.endNode,
			inputFiles:      n.endNode.outputFiles,
			mapperBatchSize: batchSize[0],
		}
		mrnode.interFiles.mrNode = mrnode
		n.graph.mrNodeIndex++
		n.endNode.chainNext = mrnode
		n.endNode = mrnode
		n.graph.allNodes = append(n.graph.allNodes, mrnode)
	} else {
		//use origin
		n.endNode.mapper = combineMappers(n.endNode.mapper, mapper)
	}
	return n
}

func (n *jobNode) AddReducer(reducer mapred.Reducer, num int) *jobNode {
	if num <= 0 {
		num = 1
	}
	if n.endNode.reducer != nil {
		mrnode := &mrNode{
			index:           n.graph.mrNodeIndex,
			jobNode:         n,
			mapper:          IdentityMapper,
			reducer:         reducer,
			chainPrev:       n.endNode,
			inputFiles:      n.endNode.outputFiles,
			mapperBatchSize: 1,
		}
		mrnode.interFiles.mrNode = mrnode
		n.graph.mrNodeIndex++
		n.endNode.chainNext = mrnode
		n.endNode = mrnode
		n.graph.allNodes = append(n.graph.allNodes, mrnode)
	} else {
		//use origin
		n.endNode.reducer = reducer
	}
	n.endNode.outputFiles = &fileNameGenerator{n.endNode, num}
	n.endNode.reducerCount = num
	return n
}

func (n *jobNode) SetName(name string) *jobNode {
	n.name = name
	return n
}

func (n *jobNode) DependOn(nodes ...*jobNode) *jobNode {
	n.dependencies = append(n.dependencies, nodes...)
	for _, node := range nodes {
		node.dependencyOf = append(node.dependencyOf, n)
	}
	return n
}

func (n *jobNode) GetMapReduceNodes() (res []*mrNode) {
	res = make([]*mrNode, 0)
	for startNode := n.startNode; startNode != nil; startNode = startNode.chainNext {
		res = append(res, startNode)
	}
	return
}

// GetDependencies return a copy of
func (n *jobNode) GetDependencies() (res []*jobNode){
	res = make([]*jobNode, len(n.dependencies))
	copy(res, n.dependencies)
	return
}

// GetDependencyOf return a copy of
func (n *jobNode) GetDependencyOf() (res []*jobNode){
	res = make([]*jobNode, len(n.dependencyOf))
	copy(res, n.dependencyOf)
	return
}

func (j *Job) AddMapper(mapper mapred.Mapper, inputs Files, batchSize ...int) *jobNode {
	if len(batchSize) == 0 {
		batchSize = append(batchSize, 1)
	}
	jnode := &jobNode{
		graph: j,
	}
	mrnode := &mrNode{
		index:           j.mrNodeIndex,
		jobNode:         jnode,
		mapper:          mapper,
		inputFiles:      inputs,
		mapperBatchSize: batchSize[0],
	}
	j.mrNodeIndex++
	jnode.startNode = mrnode
	jnode.endNode = mrnode
	mrnode.outputFiles = &fileNameGenerator{mrnode, 0}
	mrnode.interFiles.mrNode = mrnode

	j.root = append(j.root, jnode)
	j.allNodes = append(j.allNodes, mrnode)
	return jnode
}

func (j *Job) AddReducer(reducer mapred.Reducer, inputs Files, num int) *jobNode {
	if num <= 0 {
		num = 1
	}
	jnode := &jobNode{
		graph: j,
	}
	mrnode := &mrNode{
		index:           j.mrNodeIndex,
		jobNode:         jnode,
		mapper:          IdentityMapper,
		reducer:         reducer,
		inputFiles:      inputs,
		mapperBatchSize: 1,
	}
	j.mrNodeIndex++
	jnode.startNode = mrnode
	jnode.endNode = mrnode
	mrnode.outputFiles = &fileNameGenerator{mrnode, num}
	mrnode.interFiles.mrNode = mrnode
	mrnode.reducerCount = num

	j.root = append(j.root, jnode)
	j.allNodes = append(j.allNodes, mrnode)
	return jnode
}

// ValidateGraph validate the graph to ensure it can be excuted
func (j *Job) ValidateGraph() {
	visitedMap := make(map[*jobNode]bool)
	jobNodeNameMap := make(map[string]bool)
	mrNodeIndexMap := make(map[int]bool)
	mapredNodeCount := 0
	nodeStack := stack{}
	var dfs func(*jobNode)
	dfs = func(node *jobNode) {
		if node.startNode == nil {
			log.Fatal("Start node in a job node should not be nil. Job:", node.name)
		}
		for _, n := range nodeStack {
			if node.startNode.index == n {
				log.Fatal("Job graph has circle around", node.name)
			}
		}
		if _, ok := visitedMap[node]; ok {
			return
		}
		visitedMap[node] = true
		if _, ok := jobNodeNameMap[node.name]; ok {
			log.Fatal("Duplicate job name", node.name)
		}
		jobNodeNameMap[node.name] = true
		// Check whether map/reduce chain is correct
		for startNode := node.startNode; startNode != nil; startNode = startNode.chainNext {
			mapredNodeCount++

			if _, ok := mrNodeIndexMap[startNode.index]; ok {
				log.Fatalf("Duplicate mrNode index %v in job %v", startNode.index, node.name)
			}
			mrNodeIndexMap[startNode.index] = true

			if len(startNode.inputFiles.GetFiles()) == 0 {
				log.Fatalf("%v-%v input file length is 0", node.name, startNode.index)
			}
			if len(startNode.outputFiles.GetFiles()) == 0 {
				log.Fatalf("%v-%v output file length is 0", node.name, startNode.index)
			}
			if startNode.chainPrev != nil && startNode.inputFiles != startNode.chainPrev.outputFiles {
				log.Fatalf("%v-%v input files doesn't equal to prev node output files", node.name, startNode.index)
			}
			if startNode.reducerCount == 0 {
				log.Fatalf("%v-%v reducer count is 0", node.name, startNode.index)
			}
			if startNode.mapperBatchSize == 0 {
				log.Fatalf("%v-%v mapper batch size is 0", node.name, startNode.index)
			}
			nMappers := len(startNode.inputFiles.GetFiles())
			if len(startNode.interFiles.getMapperOutputFiles(0))*
				len(startNode.interFiles.getReducerInputFiles(0)) != nMappers*startNode.reducerCount {
				log.Fatalf("%v-%v inter file len is not right 0", node.name, startNode.index)
			}
			if startNode.mapper == nil {
				log.Fatalf("%v-%v doesn't have mapper", node.name, startNode.index)
			}
			if startNode.reducer == nil {
				log.Fatalf("%v-%v doesn't have reducer", node.name, startNode.index)
			}
		}
		nodeStack = nodeStack.Push(node.startNode.index)
		for _, dep := range node.dependencyOf {
			dfs(dep)
		}
		nodeStack, _ = nodeStack.Pop()
	}

	for _, node := range j.root {
		dfs(node)
	}

	if mapredNodeCount != len(j.allNodes) {
		log.Fatal("There is some orphan mapred node in the graph")
	}
}

func (j *Job) GetMapReduceNode(jobNodeName string, mapredIndex int) *mrNode {
	for _, node := range j.allNodes {
		if node.jobNode.name == jobNodeName && node.index == mapredIndex {
			return node
		}
	}
	return nil
}

func (j *Job) SetName(name string) {
	for _, c := range []rune(name) {
		if !unicode.IsLower(c) || c == rune('-') {
			log.Fatal("Job name should only contain lowercase and '-'")
		}
	}
	j.name = name
}
