package job

import (
	"sync"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/master"
	"github.com/naturali/kmr/util/log"
)

var (
	randomSeed  = flag.Int64("seed", int64(time.Now().Nanosecond()), "The random seed for master and workers")
	workerNum   = flag.Int("worker-num", 16, "How many worker you want to start")
	workerIDCmd = flag.Int64("worker-id", int64(-1), "Define it if it is worker")
)

// TODO: Get workerID and master addr from argument
var (
	workerIDs  []int64
	masterAddr = "localhost:50051"
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

type interFileNameGenerator struct {
	mrNode *mapredNode
}

func (i *interFileNameGenerator) getFile(mapperIdx, reducerIdx int) string {
	if i.mrNode == nil {
		log.Fatal("mrNode is not set")
	}
	nMappers := (len(i.mrNode.inputFiles.GetFiles()) + i.mrNode.mapperBatchSize - 1) / i.mrNode.mapperBatchSize
	nReducer := i.mrNode.reducerCount
	if !(reducerIdx >= 0 && reducerIdx < nReducer) {
		log.Fatal("SubIdx is error", reducerIdx, "when get reducer output files for job", i.mrNode.jobNode.name)
	}
	if !(mapperIdx >= 0 && mapperIdx < nMappers) {
		log.Fatal("SubIdx is error", mapperIdx, "when get mapper output files for job", i.mrNode.jobNode.name)
	}
	return fmt.Sprintf("inter-%v-%v-%v", i.mrNode.jobNode.name, i.mrNode.index, mapperIdx*nReducer+reducerIdx)
}

func (i *interFileNameGenerator) getMapperOutputFiles(mapperIdx int) []string {
	if i.mrNode == nil {
		log.Fatal("mrNode is not set")
	}
	res := make([]string, i.mrNode.reducerCount)
	for reducerIdx := range res {
		res[reducerIdx] = i.getFile(mapperIdx, reducerIdx)
	}
	return res
}

func (i *interFileNameGenerator) getReducerInputFiles(reducerIdx int) []string {
	if i.mrNode == nil {
		log.Fatal("mrNode is not set")
	}
	nMappers := (len(i.mrNode.inputFiles.GetFiles()) + i.mrNode.mapperBatchSize - 1) / i.mrNode.mapperBatchSize
	res := make([]string, nMappers)
	for mapperIdx := range res {
		res[mapperIdx] = i.getFile(mapperIdx, reducerIdx)
	}
	return res
}

type Files interface {
	GetFiles() []string
	GetType() string
}

type InputFiles struct {
	Files []string
	Type  string
}

func (f *InputFiles) GetFiles() []string {
	return f.Files
}

func (f *InputFiles) GetType() string {
	return f.Type
}

type fileNameGenerator struct {
	mrNode    *mapredNode
	fileCount int
}

func (f *fileNameGenerator) GetFiles() []string {
	res := make([]string, f.fileCount)
	for i := range res {
		res[i] = fmt.Sprintf("output-%v-%v-%v", f.mrNode.jobNode.name, f.mrNode.index, i)
	}
	return res
}

func (f *fileNameGenerator) GetType() string {
	return "stream"
}

const (
	jobNodeIdle = iota
	jobNodeProcessing
	jobNodeCompleted
)

type jobNode struct {
	sync.Mutex
	name                       string
	jobStatus                  int
	startNode, endNode         *mapredNode
	dependencies, dependencyOf []*jobNode
	graph                      *JobGraph

	currentProcessingNode *mapredNode
}

type JobConfig struct {
	mapBucket, interBucket, reduceBucket BucketDescription
	WorkerNum                            int
}

type JobGraph struct {
	root          []*jobNode
	allNodes      []*mapredNode
	mapredNodeIdx int

	Name   string
	master *master.Master
	config *JobConfig

	mapBucket, interBucket, reduceBucket bucket.Bucket
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
		n.endNode.mapper = mapred.CombineMappers(n.endNode.mapper, mapper)
	}
	return n
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
		mapper:          mapred.IdentityMapper,
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

func (n *jobNode) AddReducer(reducer mapred.Reducer, num int) *jobNode {
	if num <= 0 {
		num = 1
	}
	if n.endNode.reducer != nil {
		mrnode := &mapredNode{
			index:           n.graph.mapredNodeIdx,
			jobNode:         n,
			mapper:          mapred.IdentityMapper,
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

func (j *JobGraph) handleArguments() error {
	return nil
}

func (j *JobGraph) runMaster() {
	var workerctl master.WorkerCtl
	port := "50051"
	//TODO: assign a proper workerctl
	workerctl = NewLocalWorkerCtl(j)

	if len(j.Name) == 0 {
		j.Name = "Anonymous KMR Job"
	}

	if j.config.WorkerNum == 0 {
		for _, node := range j.root {
			j.config.WorkerNum += len(node.startNode.inputFiles.GetFiles())
		}
		if j.config.WorkerNum == 0 {
			//XXX: Why no inputs?
			j.config.WorkerNum = 3
		}
	}

	m := master.NewMaster(port, workerctl, j.Name, j.config.WorkerNum)
	j.master = m

	waitForAll := &sync.WaitGroup{}
	// topo sort and push job to master
	var topo func(node *jobNode)
	topo = func(node *jobNode) {
		for startNode := node.startNode; startNode != nil; startNode = startNode.chainNext {
			jobDesc := j.convertNodeToJobDesc(startNode)
			if jobDesc == nil {
				log.Fatal("Failed to convert node ", j.Name)
			}
			wait := j.master.PushJob(jobDesc)
			<-wait
			for x := 0; x < startNode.reducerCount; x++ {
				for _, file := range startNode.interFiles.getReducerInputFiles(x) {
					j.reduceBucket.Delete(file)
				}
			}
			if startNode.chainPrev != nil {
				for _, file := range startNode.chainPrev.outputFiles.GetFiles() {
					j.reduceBucket.Delete(file)
				}
			}
		}
		node.Lock()
		node.jobStatus = jobNodeCompleted
		node.Unlock()
		for _, nextJobNode := range node.dependencyOf {
			allDepCompleted := true
			for _, dep := range nextJobNode.dependencies {
				if dep.jobStatus != jobNodeCompleted {
					allDepCompleted = false
				}
			}
			if !allDepCompleted {
				continue
			}
			nextJobNode.Lock()
			if nextJobNode.jobStatus != jobNodeIdle {
				nextJobNode.Unlock()
				continue
			}
			nextJobNode.jobStatus = jobNodeProcessing
			nextJobNode.Unlock()
			waitForAll.Add(1)
			go topo(nextJobNode)
		}
		waitForAll.Done()
	}

	for i := range j.root {
		node := j.root[i]
		if len(node.dependencies) != 0 {
			continue
		}
		node.jobStatus = jobNodeProcessing
		waitForAll.Add(1)
		go topo(node)
	}
	waitForAll.Wait()

	log.Info("All jobs is completed")
	os.Exit(0)
}

func (j *JobGraph) convertNodeToJobDesc(node *mapredNode) *master.JobDescription {
	if node.isIntermediaNode() {

	}
	return &master.JobDescription{
		JobNodeName:      node.jobNode.name,
		MapredNodeIndex:  int32(node.index),
		MapperObjectSize: len(node.inputFiles.GetFiles()),
		MapperBatchSize:  node.mapperBatchSize,
		ReducerNumber:    node.reducerCount,
	}
}

func (node *mapredNode) isIntermediaNode() bool {
	isIntermediaNode := node != node.jobNode.endNode && node.jobNode.startNode != node.jobNode.endNode
	return isIntermediaNode
}

func (j *JobGraph) Run() {
	flag.Parse()
	//TODO: Check arguments to
	//判断是master还是worker

	rand.Seed(*randomSeed)

	//TODO Read from argument and file
	bk := BucketDescription{
		BucketType: "filesystem",
		Config: map[string]interface{}{
			"directory": "/tmp/kmrtest",
		},
	}

	j.config = &JobConfig{
		bk,
		bk,
		bk,
		*workerNum,
	}

	var err1, err2, err3 error
	j.mapBucket, err1 = bucket.NewBucket(j.config.mapBucket.BucketType, j.config.mapBucket.Config)
	j.interBucket, err2 = bucket.NewBucket(j.config.interBucket.BucketType, j.config.interBucket.Config)
	j.reduceBucket, err3 = bucket.NewBucket(j.config.reduceBucket.BucketType, j.config.reduceBucket.Config)

	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatal("Falied to create bucket", err1, err2, err3)
	}

	workerIDs = make([]int64, j.config.WorkerNum)
	workerIDMap := make(map[int64]int)
	for i := 0; i < j.config.WorkerNum; i++ {
		randWorkerID := rand.Int63()
		// Ensure it is unique
		for {
			if _, ok := workerIDMap[randWorkerID]; ok {
				randWorkerID = rand.Int63()
			} else {
				break
			}
		}
		workerIDMap[randWorkerID] = 1
		workerIDs[i] = randWorkerID
	}

	j.ValidateGraph()

	if *workerIDCmd >= 0 {
		if _, ok := workerIDMap[*workerIDCmd]; !ok {
			log.Fatal("worker id", *workerIDCmd, "doesn't exists given seed", *randomSeed)
		}
		w := worker{
			j,
			*workerIDCmd,
			60,
		}
		w.runWorker()
	} else {
		j.runMaster()
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
