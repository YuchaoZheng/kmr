package jobgraph

import (
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/util/log"
)

const (
	TaskNodeTypeMapReduce = iota
	TaskNodeTypeFilter
	TaskNodeTypeBash
)

type TaskNode interface {
	IsEndNode() bool
	GetPrev() TaskNode
	setPrev(TaskNode)
	GetNext() TaskNode
	setNext(TaskNode)
	GetIndex() int
	GetJobNode() *JobNode
	GetNodeType() int
	GetInputFiles() Files
	GetOutputFiles() Files
	Validate()
	GetPhaseCount() int
	GetTaskCountOfPhase(phase int) int
	ToJobDesc() *JobDescription
}
type TaskNodeBase struct {
	index                   int
	chainPrev, chainNext    TaskNode
	jobNode                 *JobNode
	inputFiles, outputFiles Files
}

func (n *TaskNodeBase) GetNext() TaskNode {
	return n.chainNext
}

func (n *TaskNodeBase) setNext(t TaskNode) {
	n.chainNext = t
}

func (n *TaskNodeBase) GetPrev() TaskNode {
	return n.chainPrev
}

func (n *TaskNodeBase) setPrev(t TaskNode) {
	n.chainPrev = t
}

func (n *TaskNodeBase) GetIndex() int {
	return n.index
}

func (n *TaskNodeBase) GetJobNode() *JobNode {
	return n.jobNode
}

func (n *TaskNodeBase) GetInputFiles() Files {
	return n.inputFiles
}

func (n *TaskNodeBase) GetOutputFiles() Files {
	return n.outputFiles
}

func (n *TaskNodeBase) IsEndNode() bool {
	endNode := n.GetJobNode().endNode
	return n.index == endNode.GetIndex()
}

func (n *TaskNodeBase) ToJobDesc() *JobDescription {
	return &JobDescription{
		JobNodeName:   n.jobNode.name,
		TaskNodeIndex: int32(n.index),
	}
}

func (n *TaskNodeBase) GetPhaseCount() int {
	return 1
}

type MapReduceNode struct {
	TaskNodeBase

	mapper   mapred.Mapper
	reducer  mapred.Reducer
	combiner mapred.Combiner

	mapperBatchSize int
	reducerCount    int
	interFiles      InterFileNameGenerator
}

func (node *MapReduceNode) GetNodeType() int {
	return TaskNodeTypeMapReduce
}

func (node *MapReduceNode) GetCombiner() mapred.Combiner {
	return node.combiner
}

func (node *MapReduceNode) GetInterFileNameGenerator() *InterFileNameGenerator {
	res := node.interFiles
	// prevent from writing
	return &res
}

func (node *MapReduceNode) GetMapperNum() int {
	if node.inputFiles == nil || node.mapperBatchSize == 0 {
		return 0
	}
	return (len(node.inputFiles.GetFiles()) + node.mapperBatchSize - 1) / node.mapperBatchSize
}

func (node *MapReduceNode) GetReducerNum() int {
	return node.reducerCount
}

func (node *MapReduceNode) GetMapper() mapred.Mapper {
	return node.mapper
}

func (node *MapReduceNode) GetReducer() mapred.Reducer {
	return node.reducer
}

func (node *MapReduceNode) GetMapperBatchSize() int {
	return node.mapperBatchSize
}

const (
	MapPhase    = iota
	ReducePhase
)

func (node *MapReduceNode) GetPhaseCount() int {
	return 2
}

func (node *MapReduceNode) GetTaskCountOfPhase(phase int) int {
	switch phase {
	case MapPhase:
		return (node.GetMapperNum() + node.GetMapperBatchSize() - 1) / node.GetMapperBatchSize()
	case ReducePhase:
		return node.GetReducerNum()
	default:
		log.Fatal("Phase cannot be", phase)
	}
	return -1
}

func (node *MapReduceNode) Validate() {
	jobNode := node.GetJobNode()
	if node.reducerCount == 0 {
		log.Fatalf("%v-%v reducer count is 0", jobNode.name, node.index)
	}
	if node.mapperBatchSize == 0 {
		log.Fatalf("%v-%v mapper batch size is 0", jobNode.name, node.index)
	}
	if len(node.interFiles.GetReducerInputFiles(0)) *
		len(node.interFiles.GetMapperOutputFiles(0)) != node.GetMapperNum()*node.GetReducerNum() {
		log.Fatalf("%v-%v inter file len is not right", jobNode.name, node.index)
	}
	if node.mapper == nil {
		log.Fatalf("%v-%v doesn't have mapper", jobNode.name, node.index)
	}
	if node.reducer == nil {
		log.Fatalf("%v-%v doesn't have reducer", jobNode.name, node.index)
	}
}

func (n *JobNode) AddMapper(mapper mapred.Mapper, batchSize int) *JobNode {
	originMr, ok := n.endNode.(*MapReduceNode)
	if !ok || originMr.reducer != nil {
		var mrnode *MapReduceNode
		if n.endNode != nil {
			mrnode = &MapReduceNode{
				TaskNodeBase: TaskNodeBase{
					jobNode:    n,
					chainPrev:  n.endNode,
					inputFiles: n.endNode.GetOutputFiles(),
				},
				mapper:          mapper,
				mapperBatchSize: batchSize,
			}

			n.endNode.GetOutputFiles().setBucketType(InterBucket)
			n.endNode.setNext(mrnode)
			n.endNode = mrnode
		} else {
			mrnode = &MapReduceNode{
				TaskNodeBase: TaskNodeBase{
					jobNode:    n,
					inputFiles: n.inputs,
				},
				mapper:          mapper,
				mapperBatchSize: batchSize,
			}
			mrnode.outputFiles = &fileNameGenerator{mrnode, 0, ReduceBucket}
			n.startNode = mrnode
			n.endNode = mrnode
			n.graph.roots = append(n.graph.roots, n)
		}
		mrnode.index = n.graph.taskNodeIndex
		n.graph.taskNodeIndex++
		mrnode.interFiles.mrNode = mrnode

		n.graph.taskNodes = append(n.graph.taskNodes, mrnode)
	} else {
		//use origin
		originMr.mapperBatchSize = batchSize
		originMr.mapper = combineMappers(originMr.mapper, mapper)
	}
	return n
}

func (n *JobNode) AddReducer(reducer mapred.Reducer, num int) *JobNode {
	originMr, ok := n.endNode.(*MapReduceNode)
	if num <= 0 {
		num = 1
	}
	if !ok || originMr.reducer != nil {
		n.AddMapper(IdentityMapper, 1)
		originMr, ok = n.endNode.(*MapReduceNode)
		if !ok || originMr == nil || originMr.GetMapper() == nil || originMr.GetReducer() != nil {
			// should never happen
			log.Fatal("Add a new mrnode failed when add reducer")
		}
	}
	originMr.reducer = reducer
	originMr.outputFiles = &fileNameGenerator{originMr, num, ReduceBucket}
	originMr.reducerCount = num
	return n
}

func (n *JobNode) SetCombiner(combiner mapred.Combiner) *JobNode {
	originMr, ok := n.endNode.(*MapReduceNode)
	if !ok {
		log.Fatal("Last node is not map reduce node, it's ", n.endNode.GetNodeType())
	}
	originMr.combiner = combiner
	return n
}

type FilterNode struct {
	TaskNodeBase
	batchSize int

	filter mapred.Filter
}

func (n *FilterNode) GetNodeType() int {
	return TaskNodeTypeFilter
}

func (n *FilterNode) GetFilter() mapred.Filter {
	return n.filter
}

func (n *FilterNode) GetBatchSize() int {
	return n.batchSize
}

func (n *FilterNode) Validate() {
	if n.filter == nil {
		log.Fatal("Filter Node doesn't have a filter function")
	}
	if n.batchSize <= 0 {
		log.Fatal("Filter Node's batchSize is negative", n.batchSize)
	}
}

func (n *FilterNode) GetTaskCountOfPhase(phase int) int {
	if phase != 0 {
		log.Fatal("Phase can only be zero in filter task node")
	}
	inputLen := len(n.GetInputFiles().GetFiles())
	return (inputLen + n.batchSize - 1) / n.batchSize
}

func (n *JobNode) AddFilter(filter mapred.Filter, batchSize int) *JobNode {
	fn := &FilterNode{
		TaskNodeBase: TaskNodeBase{
			index:     n.graph.taskNodeIndex,
			jobNode:   n,
			chainPrev: n.endNode,
		},
		batchSize: batchSize,
		filter:    filter,
	}
	n.graph.taskNodeIndex++
	if n.startNode == nil {
		fn.inputFiles = n.inputs
		n.startNode = fn
	} else {
		fn.inputFiles = n.endNode.GetOutputFiles()
		fn.inputFiles.setBucketType(InterBucket)
		fn.setPrev(n.endNode)
		n.endNode.setNext(fn)
	}
	n.graph.taskNodes = append(n.graph.taskNodes, fn)
	fn.outputFiles = &fileNameGenerator{fn, len(fn.inputFiles.GetFiles()), ReduceBucket}
	n.endNode = fn
	return n
}

type BashNode struct {
	TaskNodeBase
	batchSize int
	run       mapred.BashCommand
}

func (n *BashNode) GetNodeType() int {
	return TaskNodeTypeBash
}

func (n *BashNode) SetRunner(runner mapred.BashCommand) {
	n.run = runner
}

func (n *BashNode) Validate() {
	return
}

func (n *BashNode) GetTaskCountOfPhase(phase int) int {
	if phase != 0 {
		log.Fatal("Phase can only be zero in bash task node")
	}
	inputLen := len(n.GetInputFiles().GetFiles())
	return (inputLen + n.batchSize - 1) / n.batchSize
}
