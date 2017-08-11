package jobgraph

import (
	"fmt"

	"github.com/naturali/kmr/util/log"
)

type Files interface {
	GetFiles() []string
	GetType() string
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
	return fmt.Sprintf("inter-%v-%v-%v", i.mrNode.jobNode.name, i.mrNode.index, mapperIdx*nReducer+reducerIdx)
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

type fileNameGenerator struct {
	mrNode    *MapReduceNode
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

// InputFiles Define input files
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
