package jobgraph

import (
	"fmt"

	"github.com/naturali/kmr/util/log"
)

type Files interface {
	GetFiles() []string
	GetType() string
	GetBucketType() int
	setBucketType(int)
}

type InterFileNameGenerator struct {
	mrNode *MapReduceNode
}

func (i *InterFileNameGenerator) GetFile(mapperIdx, reducerIdx int) string {
	if i.mrNode == nil {
		log.Fatal("MapReduceNode is not set")
	}
	nMappers := (len(i.mrNode.inputFiles.GetFiles()) + i.mrNode.mapperBatchSize - 1) / i.mrNode.mapperBatchSize
	nReducer := i.mrNode.reducerCount
	if !(reducerIdx >= 0 && reducerIdx < nReducer) {
		log.Fatal("SubIdx is error", reducerIdx, "when get reducer output files for job", i.mrNode.jobNode.name)
	}
	if !(mapperIdx >= 0 && mapperIdx < nMappers) {
		log.Fatal("SubIdx is error", mapperIdx, "when get mapper output files for job", i.mrNode.jobNode.name)
	}
	jobName := fmt.Sprintf("inter-job-%v", i.mrNode.jobNode.name)
	mapIndex := fmt.Sprintf("inter-map-%v-%v", i.mrNode.index, mapperIdx)
	filename := fmt.Sprintf("inter-%v-%v-%v", i.mrNode.jobNode.name, i.mrNode.index, mapperIdx*nReducer+reducerIdx)
	return fmt.Sprintf("%s/%s/%s", jobName, mapIndex, filename)
}

func (i *InterFileNameGenerator) GetMapperOutputFiles(mapperIdx int) []string {
	if i.mrNode == nil {
		log.Fatal("MapReduceNode is not set")
	}
	res := make([]string, i.mrNode.reducerCount)
	for reducerIdx := range res {
		res[reducerIdx] = i.GetFile(mapperIdx, reducerIdx)
	}
	return res
}

func (i *InterFileNameGenerator) GetReducerInputFiles(reducerIdx int) []string {
	if i.mrNode == nil {
		log.Fatal("MapReduceNode is not set")
	}
	nMappers := (len(i.mrNode.inputFiles.GetFiles()) + i.mrNode.mapperBatchSize - 1) / i.mrNode.mapperBatchSize
	res := make([]string, nMappers)
	for mapperIdx := range res {
		res[mapperIdx] = i.GetFile(mapperIdx, reducerIdx)
	}
	return res
}

const (
	MapBucket    = iota
	ReduceBucket
	InterBucket
)

type fileNameGenerator struct {
	taskNode   TaskNode
	fileCount  int
	bucketType int
}

func (f *fileNameGenerator) GetFiles() []string {
	res := make([]string, f.fileCount)
	for i := range res {
		res[i] = fmt.Sprintf("output-%v-%v-%v", f.taskNode.GetJobNode().name, f.taskNode.GetIndex(), i)
	}
	return res
}

func (f *fileNameGenerator) GetType() string {
	return "stream"
}

func (f *fileNameGenerator) GetBucketType() int {
	return f.bucketType
}

func (f *fileNameGenerator) setBucketType(t int) {
	f.bucketType = t
}

// InputFiles Define input files
type InputFiles struct {
	Files      []string
	Type       string
	// Default: MapBucket
	BucketType int
}

func (f *InputFiles) GetFiles() []string {
	return f.Files
}

func (f *InputFiles) GetType() string {
	return f.Type
}

func (f *InputFiles) GetBucketType() int {
	return f.BucketType
}

func (f *InputFiles) setBucketType(int) {
	log.Panic("InputFiles doesn't have setBucketType api")
}
