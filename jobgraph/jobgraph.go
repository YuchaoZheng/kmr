package jobgraph

import (
	"unicode"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/master"
	"github.com/naturali/kmr/util/log"
)

type mapredNode struct {
	index   int
	mapper  mapred.Mapper
	reducer mapred.Reducer
	jobNode *jobNode

	mapperReaderType        string
	mapperBatchSize         int
	reducerCount            int
	interFiles              interFileNameGenerator
	inputFiles, outputFiles Files

	chainPrev, chainNext *mapredNode
}

type jobNode struct {
	name                       string
	startNode, endNode         *mapredNode
	dependencies, dependencyOf []*jobNode
	graph                      *JobGraph
}

type JobGraph struct {
	root          []*jobNode
	allNodes      []*mapredNode
	mapredNodeIdx int

	name   string
	master *master.Master

	mapBucket, interBucket, reduceBucket bucket.Bucket
	workerNum int
}

func (node *mapredNode) isIntermediaNode() bool {
	isIntermediaNode := node != node.jobNode.endNode && node.jobNode.startNode != node.jobNode.endNode
	return isIntermediaNode
}


func (n *jobNode) AddMapper(mapper mapred.Mapper, inputs Files, batchSize ...int) *jobNode {
	if len(batchSize) == 0 {
		batchSize = append(batchSize, 1)
	}
	if n.endNode.reducer != nil {
		mrnode := &mapredNode{
			index:           n.graph.mapredNodeIdx,
			jobNode:         n,
			mapper:          mapper,
			chainPrev:       n.endNode,
			inputFiles:      n.endNode.outputFiles,
			mapperBatchSize: batchSize[0],
		}
		mrnode.interFiles.mrNode = mrnode
		n.graph.mapredNodeIdx++
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
		mrnode := &mapredNode{
			index:           n.graph.mapredNodeIdx,
			jobNode:         n,
			mapper:          IdentityMapper,
			reducer:         reducer,
			chainPrev:       n.endNode,
			inputFiles:      n.endNode.outputFiles,
			mapperBatchSize: 1,
		}
		mrnode.interFiles.mrNode = mrnode
		n.graph.mapredNodeIdx++
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

func (j *JobGraph) AddMapper(mapper mapred.Mapper, inputs Files, batchSize ...int) *jobNode {
	if len(batchSize) == 0 {
		batchSize = append(batchSize, 1)
	}
	jnode := &jobNode{
		graph: j,
	}
	mrnode := &mapredNode{
		index:           j.mapredNodeIdx,
		jobNode:         jnode,
		mapper:          mapper,
		inputFiles:      inputs,
		mapperBatchSize: batchSize[0],
	}
	j.mapredNodeIdx++
	jnode.startNode = mrnode
	jnode.endNode = mrnode
	mrnode.outputFiles = &fileNameGenerator{mrnode, 0}
	mrnode.interFiles.mrNode = mrnode

	j.root = append(j.root, jnode)
	j.allNodes = append(j.allNodes, mrnode)
	return jnode
}

func (j *JobGraph) AddReducer(reducer mapred.Reducer, inputs Files, num int) *jobNode {
	if num <= 0 {
		num = 1
	}
	jnode := &jobNode{
		graph: j,
	}
	mrnode := &mapredNode{
		index:           j.mapredNodeIdx,
		jobNode:         jnode,
		mapper:          IdentityMapper,
		reducer:         reducer,
		inputFiles:      inputs,
		mapperBatchSize: 1,
	}
	j.mapredNodeIdx++
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
func (j *JobGraph) ValidateGraph() {
	visitedMap := make(map[*jobNode]bool)
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
		// Check whether map/reduce chain is correct
		for startNode := node.startNode; startNode != nil; startNode = startNode.chainNext {
			mapredNodeCount++
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

func (j *JobGraph) convertNodeToJobDesc(node *mapredNode) *JobDescription {
	if node.isIntermediaNode() {

	}
	return &JobDescription{
		JobNodeName:      node.jobNode.name,
		MapredNodeIndex:  int32(node.index),
		MapperObjectSize: len(node.inputFiles.GetFiles()),
		MapperBatchSize:  node.mapperBatchSize,
		ReducerNumber:    node.reducerCount,
	}
}

func (j *JobGraph) loadBucket(m, i, r *BucketDescription) {
	var err1, err2, err3 error
	j.mapBucket, err1 = bucket.NewBucket(m.BucketType, m.Config)
	j.interBucket, err2 = bucket.NewBucket(i.BucketType, i.Config)
	j.reduceBucket, err3 = bucket.NewBucket(r.BucketType, r.Config)

	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatal("Falied to create bucket", err1, err2, err3)
	}
}

func (j *JobGraph) getMapredNode(jobNodeName string, mapredIndex int) *mapredNode {
	for _, node := range j.allNodes {
		if node.jobNode.name == jobNodeName && node.index == mapredIndex {
			return node
		}
	}
	return nil
}

func (j *JobGraph) SetName(name string) {
	for _, c := range []rune(name) {
		if !unicode.IsLower(c) || c == rune('-') {
			log.Fatal("Job name should only contain lowercase and '-'")
		}
	}
	j.name = name
}
