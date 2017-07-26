package main

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/mapred"
)

const (
	MAX_WORD_LENGTH = 20
)

type WordCountMap struct {
	mapred.MapReduceBase
}

type WordCountReduce struct {
	mapred.MapReduceBase
}

func (*WordCountMap) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	v, _ := value.(string)
	for _, procceed := range ProcessSingleSentence(strings.Trim(v, "\n")) {
		if len(procceed) > MAX_WORD_LENGTH {
			continue
		}
		output(procceed, uint64(1))
	}
}

func (*WordCountReduce) Reduce(key interface{}, valuesNext func() (interface{}, error), output func(k interface{}, v interface{}), reporter interface{}) {
	var count uint64
	mapred.ForEachValue(valuesNext, func(value interface{}) {
		val, _ := value.(uint64)
		count += val
	})
	fmt.Println(key, " ", count)
	output(key, count)
}

func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

func ProcessSingleSentence(line string) []string {
	outputs := make([]string, 0)
	e_word := ""
	for _, r := range line {
		if isChinese(r) {
			if len(e_word) > 0 {
				outputs = append(outputs, e_word)
				e_word = ""
			}
			outputs = append(outputs, string(r))
		} else if isAlphaOrNumber(r) {
			e_word += string(r)
		} else {
			if len(e_word) > 0 {
				outputs = append(outputs, e_word)
				e_word = ""
			}
		}
	}
	if len(e_word) > 0 {
		outputs = append(outputs, e_word)
	}
	return outputs
}

func main() {
	wcmap := &WordCountMap{
		mapred.MapReduceBase{
			InputKeyTypeConverter:    mapred.Int32{},
			InputValueTypeConverter:  mapred.String{},
			OutputKeyTypeConverter:   mapred.String{},
			OutputValueTypeConverter: mapred.Uint64{},
		},
	}
	wcreduce := &WordCountReduce{
		mapred.MapReduceBase{
			InputKeyTypeConverter:    mapred.String{},
			InputValueTypeConverter:  mapred.Uint64{},
			OutputKeyTypeConverter:   mapred.String{},
			OutputValueTypeConverter: mapred.Uint64{},
		},
	}
	cw := &executor.ComputeWrapClass{}
	cw.BindMapper(wcmap)
	cw.BindReducer(wcreduce)
	cw.Run()
}
