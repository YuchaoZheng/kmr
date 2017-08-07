package job

import (
	"sync"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/master"
	"github.com/naturali/kmr/util/log"

	"github.com/urfave/cli"
	"path"
	"os/user"
	"k8s.io/client-go/rest"
	"net"
	"strconv"
	"encoding/json"
	"io/ioutil"
	"github.com/naturali/kmr/util"
	"path/filepath"
	"errors"
)

// TODO: Get workerID and master addr from argument
var (
	workerIDs  []int64
	workerIDMap = make(map[int64]int)
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

	mapBucket, interBucket, reduceBucket bucket.Bucket
	workerNum int
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

func (j *JobGraph) runMaster(workerctl master.WorkerCtl, port string) {
	if len(j.Name) == 0 {
		j.Name = "Anonymous KMR Job"
	}

	m := master.NewMaster(port, workerctl, j.Name, j.workerNum)
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

func (j *JobGraph) loadBucket(m, i, r *BucketDescription) {
	var err1, err2, err3 error
	j.mapBucket, err1 = bucket.NewBucket(m.BucketType, m.Config)
	j.interBucket, err2 = bucket.NewBucket(i.BucketType, i.Config)
	j.reduceBucket, err3 = bucket.NewBucket(r.BucketType, r.Config)

	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatal("Falied to create bucket", err1, err2, err3)
	}
}

func (j *JobGraph) loadFromLocalConfig(config *LocalConfig) {
	if config.MapBucket == nil || config.ReduceBucket == nil || config.InterBucket == nil {
		log.Fatal("Lack bucket config")
	}
	j.loadBucket(config.MapBucket, config.InterBucket, config.ReduceBucket)
}

func (j *JobGraph) loadFromRemoteConfig(config *RemoteConfig) {
	if config.MapBucket == nil || config.ReduceBucket == nil || config.InterBucket == nil {
		log.Fatal("Lack bucket config")
	}
	j.loadBucket(config.MapBucket, config.InterBucket, config.ReduceBucket)
}

func (j* JobGraph) initWorkerIDs(num int, seed int64) {
	var finalSeed int64
	fmt.Sscanf(fmt.Sprintf("%v%v", num, seed), "%v", &finalSeed)
	rand.Seed(finalSeed)
	j.workerNum = num
	workerIDs = make([]int64, j.workerNum)
	for i := 0; i < j.workerNum; i++ {
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
}

func (j *JobGraph) Run() {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	configFiles := []string {
		"/etc/kmr/config.json",
		path.Join(usr.HomeDir, ".config/kmr/config.json"),
		"./config.json",
	}
	var config *Config

	app := cli.NewApp()
	app.Name = j.Name
	app.Description = "A KMR application named " + j.Name
	app.Author = "Naturali"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag {
		cli.StringSliceFlag{
			Name:"config",
			Usage:"Load config from `FILES`",
		},
		cli.Int64Flag{
			Name: "random-seed",
			Value: time.Now().Unix(),
			Usage: "Used to synchronize information between workers and master",
		},
	}
	app.Before = func(c *cli.Context) error {
		config = j.loadConfigFromMultiFiles(append(configFiles, c.StringSlice("config")...)...)
		j.ValidateGraph()
		return nil
	}

	app.Commands = []cli.Command {
		{
			Name: "master",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name: "remote",
					Usage: "Run on kubernetes",
				},
				cli.IntFlag{
					Name:"port",
					Value:50051,
					Usage:"Define how many workers",
				},
				cli.IntFlag{
					Name:"worker-num",
					Value:1,
					Usage:"Define how many workers",
				},
				cli.BoolFlag{
					Name: "listen-only",
					Usage: "Listen and waiting for workers",
				},
				cli.IntFlag{
					Name: "cpu-limit",
					Value: 1,
					Usage: "Define worker cpu limit when run in k8s",
				},
			},
			Before: func(ctx *cli.Context) error {
				workerNum := ctx.Int("worker-num")
				j.initWorkerIDs(workerNum, ctx.GlobalInt64("random-seed"))
				return nil
			},
			Action: func(ctx *cli.Context) error {
				var workerCtl master.WorkerCtl
				seed := ctx.GlobalInt64("random-seed")
				if ctx.Bool("remote") {
					j.loadFromRemoteConfig(config.Remote)

					var k8sconfig *rest.Config

					k8sSchema := os.Getenv("KUBERNETES_API_SCHEMA")
					if k8sSchema != "http" { // InCluster
						k8sconfig, err = rest.InClusterConfig()
						if err != nil {
							log.Fatalf("Can't get incluster config, %v", err)
						}
					} else { // For debug usage. > source dev_environment.sh
						host := os.Getenv("KUBERNETES_SERVICE_HOST")
						port := os.Getenv("KUBERNETES_SERVICE_PORT")

						k8sconfig = &rest.Config{
							Host: fmt.Sprintf("%s://%s", k8sSchema, net.JoinHostPort(host, port)),
						}
						token := os.Getenv("KUBERNETES_API_ACCOUNT_TOKEN")
						if len(token) > 0 {
							k8sconfig.BearerToken = token
						}
					}
					workerCtl = master.NewK8sWorkerCtl(&master.K8sWorkerConfig{
							Name: j.Name,
							CPULimit: "1",
							Namespace: *config.Remote.Namespace,
							K8sConfig: *k8sconfig,
							WorkerNum: ctx.Int("worker-num"),
							Volumes: *config.Remote.PodDesc.Volumes,
							VolumeMounts: *config.Remote.PodDesc.VolumeMounts,
							WorkerIDs: workerIDs,
							RandomSeed: seed,
						})
				} else {
					j.loadFromLocalConfig(config.Local)
					workerCtl = NewLocalWorkerCtl(j, ctx.Int("port"))
				}

				if ctx.Bool("listen-only") {
					workerCtl = nil
					fmt.Println("Listen only mode")
					fmt.Println("Seed: ", seed)
					fmt.Println("WorkerIDs: ")
					for _, id := range workerIDs {
						fmt.Println(id)
					}
					fmt.Println("Use following command to start a worker")
					for _, id := range workerIDs {
						fmt.Println(os.Args[0], "--random-seed", seed,
						"worker", "--worker-id", id, "--worker-num", j.workerNum)
					}
				}

				j.runMaster(workerCtl, strconv.Itoa(ctx.Int("port")))
				return nil
			},
		},
		{
			Name: "worker",
			Flags: []cli.Flag{
				cli.Int64Flag{
					Name:"worker-id",
					Usage: "Define worker's ID",
				},
				cli.StringFlag{
					Name: "master-addr",
					Value: "localhost:50051",
					Usage: "Master's address, format: \"HOSTNAME:PORT\"",
				},
				cli.IntFlag{
					Name:"worker-num",
					Value:1,
					Usage:"Define how many workers",
				},
				cli.BoolFlag{
					Name: "local",
					Usage: "This worker will read local config",
				},
			},
			Usage: "Run in remote worker mode, this will read remote config items",
			Before: func(ctx *cli.Context) error {
				workerNum := ctx.Int("worker-num")
				j.initWorkerIDs(workerNum, ctx.GlobalInt64("random-seed"))
				return nil
			},
			Action: func(ctx *cli.Context) error {
				workerID := ctx.Int64("worker-id")
				if _, ok := workerIDMap[workerID]; !ok {
					return cli.NewExitError(
						fmt.Sprintln("worker id", workerID ,
							"doesn't exists given seed",
							ctx.GlobalInt64("random-seed"), workerIDMap), -1)
				}
				if ctx.Bool("local") {
					j.loadFromLocalConfig(config.Local)
				} else {
					j.loadFromRemoteConfig(config.Remote)
				}
				w := worker{
					j,
					workerID,
					60,
					ctx.String("master-addr"),
				}
				w.runWorker()
				return nil
			},
		},
		{
			Name: "deploy",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:"port",
					Value:50051,
					Usage:"Define how many workers",
				},
				cli.IntFlag{
					Name:"worker-num",
					Value:1,
					Usage:"Define how many workers",
				},
				cli.IntFlag{
					Name: "cpu-limit",
					Value: 1,
					Usage: "Define worker cpu limit when run in k8s",
				},
				cli.StringSliceFlag{
					Name: "image-tags",
					Value: &cli.StringSlice{"kmr"},
				},
				cli.StringFlag{
					Name: "asset-folder",
					Value: "./assets",
					Usage: "Files under asset folder will be packed into docker image. " +
						"KMR APP can use this files as they are under './'",
				},
			},
			Usage: "Deploy KMR Application in k8s",
			Action: func(ctx *cli.Context) error {
				dockerWorkDir := "/kmrapp"
				// collect files under current folder
				assetFolder, err := filepath.Abs(ctx.String("asset-folder"))
				if err != nil {
					return err
				}
				if f, err := os.Stat(assetFolder); err != nil || !f.IsDir() {
					if os.IsNotExist(err) {
						err := os.MkdirAll(assetFolder, 0777)
						if err != nil {
							return cli.NewMultiError(err)
						}
						defer os.RemoveAll(assetFolder)
					} else {
						return cli.NewExitError("Asset folder " + assetFolder + " is incorrect", 1)
					}
				}
				log.Info("Asset folder is", assetFolder)

				configFilePath := path.Join(assetFolder, "internal-used-config.json")
				executablePath := path.Join(assetFolder, "internal-used-executable")

				if _, err := os.Stat(configFilePath); err == nil {
					return cli.NewExitError(configFilePath + "should not exists", 1)
				}
				if _, err := os.Stat(executablePath); err == nil {
					return cli.NewExitError(executablePath + "should not exists", 1)
				}

				configJson, err := json.MarshalIndent(config, "", "\t")
				if err != nil {
					return err
				}
				err = ioutil.WriteFile(configFilePath, configJson,0666)
				defer os.Remove(configFilePath)
				if err != nil {
					return err
				}

				//TODO Use Ermine to pack executable and dynamic library
				exe, err1 := os.Executable()
				exe, err2 := filepath.EvalSymlinks(exe)
				if err1 != nil || err2 != nil {
					return cli.NewMultiError(errors.New("cannot locate executable"), err1, err2)
				}
				os.Link(exe, executablePath)
				defer os.Remove(executablePath)


				tags := ctx.StringSlice("image-tags")
				if len(tags) == 0 {
					tags = append(tags, j.Name + ":" + "latest")
				}

				files, err := filepath.Glob(assetFolder+"/*")
				if err != nil {
					return err
				}

				imageName, err := util.CreateDockerImage( *config.Remote.DockerRegistry, tags, files, dockerWorkDir)
				if err != nil {
					return err
				}

				pod, service, err := util.CreateK8sKMRJob(j.Name,
					*config.Remote.ServiceAccount,
					*config.Remote.Namespace,
					*config.Remote.PodDesc, imageName, dockerWorkDir,
					[]string{},
					int32(ctx.Int("port")))

				if err != nil {
					return cli.NewMultiError(err)
				}
				log.Info("Pod: ", pod, "Service: ", service)
				return nil
			},
		},
	}

	app.Run(os.Args)
	os.Exit(0)
}

func (j *JobGraph) getMapredNode(jobNodeName string, mapredIndex int) *mapredNode {
	for _, node := range j.allNodes {
		if node.jobNode.name == jobNodeName && node.index == mapredIndex {
			return node
		}
	}
	return nil
}
