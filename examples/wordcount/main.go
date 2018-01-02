package main

import (
	"strings"
	"unicode"

	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
)

const maxWordLength = 20

// isAlphaOrNumber determines whether a rune is a digit or english character.
func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

// isChinese determines whether a rune is a Chinese.
func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

// tokenizeWords splits the line into multiple words. It only keeps Chinese characters, English word and numbers.
// It ignores all of invalid characters.
// For example:
// 		tokenizeWords("我的iphone7 is mine!!!!!") = []string{"我", "的", "iphone7", "is", "mine"}
func tokenizeWords(line string) []string {
	outputs := make([]string, 0)
	englishWord := ""
	for _, r := range line {
		if isChinese(r) {
			if len(englishWord) > 0 {
				outputs = append(outputs, englishWord)
				englishWord = ""
			}
			outputs = append(outputs, string(r))
		} else if isAlphaOrNumber(r) {
			englishWord += string(r)
		} else {
			if len(englishWord) > 0 {
				outputs = append(outputs, englishWord)
				englishWord = ""
			}
		}
	}
	if len(englishWord) > 0 {
		outputs = append(outputs, englishWord)
	}
	return outputs
}

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
func (*wordCountMap) Map(key interface{}, value interface{},
	output func(k, v interface{}), reporter interface{}) {
	v, _ := value.(string)
	for _, procceed := range tokenizeWords(strings.Trim(v, "\n")) {
		if len(procceed) > maxWordLength {
			continue
		}
		output(procceed, uint64(1))
	}
}

// Reduce key is word and valueNext is an iterator function. Add all values of one key togather to count the word occurs
func (*wordCountReduce) Reduce(key interface{}, valuesNext mapred.ValueIterator,
	output func(v interface{}), reporter interface{}) {
	var count uint64
	mapred.ForEachValue(valuesNext, func(value interface{}) {
		val, _ := value.(uint64)
		count += val
	})
	output(count)
}

func (*wordCountCombine) Combine(key interface{}, v1 interface{}, v2 interface{}, output func(v interface{})) {
	output(v1.(uint64) + v2.(uint64))
}

// It defines the map-reduce of word-count which is counting the number of each word show-ups in the corpus.
func NewWordCountMapReduce() (*wordCountMap, *wordCountReduce, *wordCountCombine) {
	wcmap := &wordCountMap{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
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
	job.SetName("wordcount")
	input := &jobgraph.InputFiles{
		Files: []string{
			"/etc/passwd",
		},
		Type: "textstream",
	}
	job.AddJobNode(input, "wordcount").
		AddMapper(mapper, 1).
		AddReducer(reducer, 1).
		SetCombiner(combiner)
	cli.Run(&job)
}
