package main

import (
	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
)

type wordCountMap struct {
	mapred.MapperCommon
}

type wordCountReduce struct {
	mapred.ReducerCommon
}

type wordCountCombine struct {
	mapred.CombineCommon
}

// Map Value is lines from file. Map function split lines into words and emit (word, 1) pairs
func (*wordCountMap) Map(
	key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	output(value.(string), uint64(1))
}

// Reduce key is word and valueNext is an iterator function. Add all values of one key togather to count the word occurs
func (*wordCountReduce) Reduce(
	key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	output(uint64(1))
}

func (*wordCountCombine) Combine(key interface{}, v1 interface{}, v2 interface{}, output func(v interface{})) {
	output(v1.(uint64) + v2.(uint64))
}

// It defines the map-reduce of word-count which is counting the number of each word show-ups in the corpus.
func NewWordCountMapReduce() (*wordCountMap, *wordCountReduce, *wordCountCombine) {
	wcmap := &wordCountMap{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint64{},
			},
		},
	}
	wcreduce := &wordCountReduce{
		ReducerCommon: mapred.ReducerCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.Uint64{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint64{},
			},
		},
	}
	wcCombine := &wordCountCombine{
		CombineCommon: mapred.CombineCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.Uint64{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint64{},
			},
		},
	}
	return wcmap, wcreduce, wcCombine
}

func main() {
	mapper, reducer, combiner := NewWordCountMapReduce()

	var job jobgraph.Job
	job.SetName("leveldb-bucket")
	input := &jobgraph.InputFiles{
		Files: []string{
			"/etc/passwd",
		},
		Type: "textstream",
	}
	job.AddJobNode(input, "leveldb-bucket").
		AddMapper(mapper, 1).
		AddReducer(reducer, 1).
			SetCombiner(combiner).
		SetOutputType("leveldb")
	cli.Run(&job)
}
