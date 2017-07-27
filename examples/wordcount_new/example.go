package main

import (
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/mapred"
)

const (
	maxWordLength = 20
)

type wordCountMap struct {
	mapred.MapperCommon
}

type wordCountReduce struct {
	mapred.ReducerCommon
}

// Map Value is lines from file. Map function split lines into words and emit (word, 1) pairs
func (*wordCountMap) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	v, _ := value.(string)
	for _, procceed := range processSingleSentence(strings.Trim(v, "\n")) {
		if len(procceed) > maxWordLength {
			continue
		}
		output(procceed, uint32(1))
	}
}

// Reduce key is word and valueNext is an iterator function. Add all values of one key togather to count the word occurs
func (*wordCountReduce) Reduce(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	var count uint32
	mapred.ForEachValue(valuesNext, func(value interface{}) {
		val, _ := value.(uint32)
		count += val
	})
	output(count)
}

func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

func processSingleSentence(line string) []string {
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

func main() {
	wcmap := &wordCountMap{
		mapred.MapperCommon{
			mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Int32{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
	}
	wcreduce := &wordCountReduce{
		mapred.ReducerCommon{
			mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Int32{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
	}
	cw := &executor.ComputeWrapClass{}
	cw.BindMapper(wcmap)
	cw.BindReducer(wcreduce)
	cw.Run()
}
