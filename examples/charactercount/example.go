package main

import (
	"unicode"
	"io"
	"fmt"

	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
)

const (
	maxWordLength = 20
)

type characterCountMap struct {
	mapred.MapperCommon
	judgeFunc func(ch rune) bool
}

type characterCountReduce struct {
	mapred.ReducerCommon
}

type reverseMapper struct {
	mapred.MapperCommon
}

type reverseReduce struct {
	mapred.ReducerCommon
}

type outputText struct {
	mapred.FilterCommon
}

func (*outputText) Filter(k, v interface{}, output io.Writer) error {
	_, err := output.Write([]byte(fmt.Sprintln(k, v)))
	return err
}

// Map Value is lines from file. Map function split lines into words and emit (word, 1) pairs
func (mapper *characterCountMap) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	v, _ := value.(string)
	for _, c := range []rune(v) {
		if mapper.judgeFunc(c) {
			output(string([]rune{c}), uint32(1))
		}
	}
}

// Reduce key is word and valueNext is an iterator function. Add all values of one key togather to count the word occurs
func (*characterCountReduce) Reduce(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	var count uint32
	mapred.ForEachValue(valuesNext, func(value interface{}) {
		val, _ := value.(uint32)
		count += val
	})
	output(count)
}

func (*reverseMapper) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	output(value, key)
}

func (*reverseReduce) Reduce(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	v, _ := valuesNext.Next()
	output(v)
}

func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

func isSpecial(r rune) bool {
	return !isAlphaOrNumber(r)
}

func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

func main() {
	wcmap := &characterCountMap{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
		judgeFunc: isAlphaOrNumber,
	}
	scmap := &characterCountMap{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
		judgeFunc: isSpecial,
	}
	rmap := &reverseMapper{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.Bytes{},
				OutputKeyTypeConverter:   mapred.Bytes{},
				OutputValueTypeConverter: mapred.Bytes{},
			},
		},
	}
	rred := &reverseReduce{
		ReducerCommon: mapred.ReducerCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.Bytes{},
				OutputKeyTypeConverter:   mapred.Bytes{},
				OutputValueTypeConverter: mapred.Bytes{},
			},
		},
	}
	wcreduce := &characterCountReduce{
		ReducerCommon: mapred.ReducerCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.Uint32{},
				OutputKeyTypeConverter:   mapred.Bytes{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
	}

	outputFilter := &outputText{
		FilterCommon: mapred.FilterCommon{
			InputTypeKeyConverters: mapred.InputTypeKeyConverters {
				InputKeyTypeConverter: mapred.Uint32{},
				InputValueTypeConverter:mapred.String{},
			},
		},
	}

	var job jobgraph.Job
	job.SetName("word-count")

	inputs := &jobgraph.InputFiles{
		// put a.t in the map bucket directory
		Files: []string{"/etc/passwd"},
		Type:  "textstream",
	}

	cc := job.AddJobNode(inputs, "CountAlphaCh").
		AddMapper(wcmap, 1).
		AddReducer(wcreduce, 10).
		AddReducer(wcreduce, 1)
	cs := job.AddJobNode(inputs, "CountSpecialCh").
		AddMapper(scmap, 1).
		AddReducer(wcreduce, 1)

	inputs2 := &jobgraph.InputFiles{
		Files: append(cc.GetOutputFiles().GetFiles(), cs.GetOutputFiles().GetFiles()...),
		Type:  "stream",
		BucketType: jobgraph.ReduceBucket,
	}

	fmt.Print(inputs2.GetFiles())
	ca := job.AddJobNode(inputs2, "CountAllCh").
		AddReducer(wcreduce, 1).
		DependOn(cc, cs)

	job.AddJobNode(ca.GetOutputFiles(), "Reverse").
		AddMapper(rmap, 1).
		AddReducer(rred, 1).
		AddFilter(outputFilter, 1).
		DependOn(ca)

	cli.Run(&job)
}
