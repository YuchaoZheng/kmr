package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/naturali/kmr-jobs/pkg/aca"

	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/count"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"

	"github.com/naturali/kmr-jobs/utils/input_file"
)

type execMap struct {
	mapred.MapperCommon
}

type execReduce struct {
	mapred.ReducerCommon
}

var acMachine = aca.NewAhoCorasickMatcher()

const numberOfType = 1

var typeName = [numberOfType]string{"actors"}
var inputFile = [numberOfType]string{"/cephfs/kmr/actors.txt"}
var hintWords = [numberOfType][]string{{"导演", "演员"}}
var hintWordsBinary = make(map[string]int)
var keyWordsOriginalName = make(map[string]string)
var keyWordsBinary = make(map[string]int)

// Map Value is lines from file. Map function split lines into words and emit (word, 1) pairs
func (*execMap) Map(key interface{}, value interface{},
	output func(k, v interface{}), counter count.CountInterface) {
	var matchHintWords [numberOfType][]string
	hintWordHasOccur := make(map[string]bool)

	acMachineSentence := []rune(strings.ToLower(key.(string)))
	acMachineWords, _ := acMachine.MatchRunes(acMachineSentence)

	for i := 0; i < len(acMachineWords); i++ {
		if !hintWordHasOccur[acMachineWords[i]] && hintWordsBinary[acMachineWords[i]] != 0 {
			hintWordHasOccur[acMachineWords[i]] = true
			binaryNum := hintWordsBinary[acMachineWords[i]]
			for ID := 0; ID < numberOfType; ID++ {
				if (binaryNum & (1 << uint(ID))) != 0 {
					matchHintWords[ID] = append(matchHintWords[ID], acMachineWords[i])
				}
			}
			counter.Add(acMachineWords[i], int64(1))
		}
	}

	wordHasOccur := make(map[string]bool)

	for i := 0; i < len(acMachineWords); i++ {
		if wordHasOccur[acMachineWords[i]] == true {
			continue
		}
		wordHasOccur[acMachineWords[i]] = true
		binaryNum := keyWordsBinary[acMachineWords[i]]
		for ID := 0; ID < numberOfType; ID++ {
			if (binaryNum & (1 << uint(ID))) != 0 {
				output(string(typeName[ID]+"\t"+keyWordsOriginalName[acMachineWords[i]]), string(" "))
				if len(matchHintWords[ID]) != 0 {
					for _, hintword := range matchHintWords[ID] {
						output(string(typeName[ID]+"\t"+keyWordsOriginalName[acMachineWords[i]]), hintword)
					}
				}
			}
		}
	}
	counter.Add("<EOF>", int64(1))
}

// Reduce key is word and valueNext is an iterator function. Add all values of one key togather to count the word occurs
func (cwReduce *execReduce) Reduce(
	key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), counter count.CountInterface) {
	countHW := make(map[string]int64)
	countTypeKeyword := int64(0)
	for v, err := valuesNext.Next(); err == nil; v, err = valuesNext.Next() {
		if v.(string) == " " {
			countTypeKeyword++
		} else {
			countHW[v.(string)]++
		}
	}
	countSentence := counter.GetValue("<EOF>").(int64)
	countHintwords := int64(0)
	countHintKeywords := int64(0)
	outputString := ""
	keySplit := strings.Split(key.(string), "\t")
	for id, typename := range typeName {
		if typename == keySplit[0] {
			for _, hintword := range hintWords[id] {
				counterHintword := counter.GetValue(hintword).(int64)
				countHintwords += counterHintword
				countHintKeywords += countHW[hintword]
				outputString += hintword + "\t" + strconv.FormatInt(countHW[hintword], 10) + "\t" +
						strconv.FormatInt(counterHintword, 10) + "\t"
			}
			break
		}
	}
	if countHintKeywords == 0 {
		countHintKeywords = 1
	}
	probability := float64(countHintKeywords)*float64(countSentence) / (float64(countTypeKeyword)*float64(countHintwords))
	logOfProbability := math.Log(probability)
	outputString += strconv.FormatInt(countHintKeywords, 10) + "\t" + strconv.FormatInt(countSentence, 10) + "\t" +
			strconv.FormatInt(countTypeKeyword, 10) + "\t" + strconv.FormatInt(countHintwords, 10) + "\t" +
			strconv.FormatFloat(probability, 'f', 6, 64) + "\t" + strconv.FormatFloat(logOfProbability, 'f', 6, 64)
	output(outputString)
}

// It defines the map-reduce of word-count which is counting the number of each word show-ups in the corpus.
func newMapReduce() (*execMap, *execReduce) {
	wcmap := &execMap{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.Bytes{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.String{},
			},
		},
	}
	wcreduce := &execReduce{
		ReducerCommon: mapred.ReducerCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.String{},
			},
		},
	}
	return wcmap, wcreduce
}

var acMachineBuildWords []string

func buildAcMachine(inputFileName string, mul int) {
	inputFile, inputError := os.Open(inputFileName)
	defer inputFile.Close()
	if inputError != nil {
		fmt.Println(inputError)
		return
	}
	inputReader := bufio.NewReader(inputFile)

	for {
		inputString, readerError := inputReader.ReadString('\n')
		inputString = strings.Trim(inputString, "\n")
		name := strings.ToLower(inputString)
		acMachineBuildWords = append(acMachineBuildWords, name)
		keyWordsOriginalName[name] = inputString
		keyWordsBinary[name] |= mul
		if readerError == io.EOF {
			break
		}
	}
}

func main() {
	mapper, reducer := newMapReduce()
	var input *jobgraph.InputFiles

	mul := 1
	for id := 0; id < numberOfType; id++ {
		buildAcMachine(inputFile[id], mul)
		for _, hintword := range hintWords[id] {
			acMachineBuildWords = append(acMachineBuildWords, hintword)
			hintWordsBinary[hintword] |= mul
		}
		mul *= 2
	}
	fmt.Println("PPPPPP", len(acMachineBuildWords))
	acMachine.Build(acMachineBuildWords)
	if false {
		input = &jobgraph.InputFiles{
			Files: []string{
				"/mnt/cephfs/kmr/pgdedup-2t-2048/res-2.t",
			},
			Type: "stream",
		}
	} else {
		input = input_file.PgDedupFullSet("stream")
	}
	var job jobgraph.Job
	job.SetName("type-2048-actors-probability")
	job.AddJobNode(input, "type-2048-actors-probability").
		AddMapper(mapper, 1).
		AddReducer(reducer, 1)
    cli.Run(&job)
}
