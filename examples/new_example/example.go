package main

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor_new"
	"github.com/naturali/kmr/mapred"
)

const (
	MAX_WORD_LENGTH = 20
)

type MyMap struct {
	mapred.MapReduceBase
}

type MyReduce struct {
	mapred.MapReduceBase
}

func (*MyMap) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	v, _ := value.(string)
	for _, procceed := range ProcessSingleSentence(strings.Trim(v, "\n")) {
		if len(procceed) > MAX_WORD_LENGTH {
			continue
		}
		output(procceed, uint64(1))
	}
}

func (*MyReduce) Reduce(key interface{}, values chan interface{}, output func(k interface{}, v interface{}), reporter interface{}) {
	var count uint64
	for v := range values {
		val, _ := v.(uint64)
		count += val
	}
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
	mymap := &MyMap{
		mapred.MapReduceBase{
			InputKeyClass:    mapred.Int32{},
			InputValueClass:  mapred.String{},
			OutputKeyClass:   mapred.String{},
			OutputValueClass: mapred.Uint64{},
		},
	}
	myreduce := &MyReduce{
		mapred.MapReduceBase{
			InputKeyClass:    mapred.String{},
			InputValueClass:  mapred.Uint64{},
			OutputKeyClass:   mapred.String{},
			OutputValueClass: mapred.Uint64{},
		},
	}
	cw := &executor_new.ComputeWrapClass{}
	cw.BindMapper(mymap)
	cw.BindReducer(myreduce)
	cw.Run()
}
