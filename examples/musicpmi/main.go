package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/pkg/aca"
	graph "github.com/naturali/kmr/job"
)

var musicMatcher, artistMatcher *aca.AhoCorasickMatcher
var musicArtistMap = make(map[string][]string)
var job = graph.JobGraph{}
var initialized = false

func Init() {
	if initialized {
		return
	}
	initialized = true
	musicMatcher = aca.NewAhoCorasickMatcher()
	artistMatcher = aca.NewAhoCorasickMatcher()
	var musicListFileDir string
	if name, err := os.Hostname(); err == nil && (name == "arch-sunsijie-linux") {
		musicListFileDir = "/home/sunsijie/kmr-jobs/output2"
	} else {
		musicListFileDir = "/cephfs"
	}
	file, err := os.Open(filepath.Join(musicListFileDir, ".", "filtered_music"))
	defer file.Close()
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)
	musics := make([]string, 0)
	artists := make([]string, 0)
	count := 0
	for {
		count++
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.Replace(line, " ", "", -1)
		splited := strings.Split(line, ",")
		if len(splited) != 2 {
			panic(fmt.Sprintln("Music csv malformated with line", line))
		}
		music := splited[0]
		artist := splited[1]
		if utf8.RuneCountInString(music) > 0 {
			musics = append(musics, music)
			musicArtistMap[music] = append(musicArtistMap[music], artist)
		}
		if utf8.RuneCountInString(artist) > 0 {
			artists = append(artists, artist)
		} else {
			artist = "Unknown"
		}
	}
	fmt.Println("total: ", count)
	musicMatcher.Build(musics)
	artistMatcher.Build(artists)
}

func CountMusicMap(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	line := strings.Replace(string(value.(string)), " ", "", -1)
	strs, _ := musicMatcher.Match(line)
	for _, m := range strs {
		output(m, uint32(1))
	}
}

func CountArtistMap(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	line := strings.Replace(string(value.(string)), " ", "", -1)
	strs, _ := artistMatcher.Match(line)
	for _, m := range strs {
		output(m, uint32(1))
	}
}

func CountPairMap(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	line := strings.Replace(string(value.(string)), " ", "", -1)
	musics, _ := artistMatcher.Match(line)
	artists, _ := musicMatcher.Match(line)
	for _, m := range musics {
		for _, artist := range artists {
			for _, artistOfMusic := range musicArtistMap[m] {
				if artist == artistOfMusic {
					output(strings.Join([]string{m, artist}, ","), uint32(1))
				}
			}
		}
	}
}

func CountAllMap(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	output("All", uint32(1))
}

func AggregateReducer(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	var counter uint32
	mapred.ForEachValue(valuesNext, func(v interface{}) {
		counter += v.(uint32)
	})
	output(counter)
}

func PMIMapper(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
}

func PMIReducer(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
}

func main() {
	job.Name = "abc"
	countMusicMapper := mapred.GetFunctionMapper(CountMusicMap, mapred.String, mapred.String, mapred.String, mapred.Uint32, Init)
	countArtistMapper := mapred.GetFunctionMapper(CountArtistMap, mapred.String, mapred.String, mapred.String, mapred.Uint32, Init)
	countPairMapper := mapred.GetFunctionMapper(CountPairMap, mapred.String, mapred.String, mapred.String, mapred.Uint32, Init)
	countAllMapper := mapred.GetFunctionMapper(CountAllMap, mapred.String, mapred.String, mapred.String, mapred.Uint32, Init)
	pmiMapper := mapred.GetFunctionMapper(PMIMapper, mapred.String, mapred.String, mapred.String, mapred.Bytes, Init)

	aggregateReducer := mapred.GetFunctionReducer(AggregateReducer, mapred.Bytes, mapred.Uint32, mapred.Bytes, mapred.Uint32, Init)
	pmiReducer := mapred.GetFunctionReducer(PMIReducer, mapred.String, mapred.Bytes, mapred.String, mapred.String, Init)

	inputSentencesFiles := &graph.InputFiles{
		[]string{"/cephfs/kmr/sentence-count-410/res-0.t"},
		"stream",
	}

	//inputMusicListFiles := &graph.InputFiles{
	//	[]string{"a.t"},
	//	"textstream",
	//}

	callj := job.AddMapper(countAllMapper, inputSentencesFiles).AddReducer(aggregateReducer, 1).AddReducer(aggregateReducer, 1).SetName("CALL")
	caj := job.AddMapper(countArtistMapper, inputSentencesFiles).AddReducer(aggregateReducer, 1).SetName("CA")
	cpj := job.AddMapper(countPairMapper, inputSentencesFiles).AddReducer(aggregateReducer, 1).SetName("CP")
	cmj := job.AddMapper(countMusicMapper, inputSentencesFiles).AddReducer(aggregateReducer, 1).SetName("CM")

	//pmij := job.AddMapper(pmiMapper, inputMusicListFiles).AddReducer(pmiReducer, 10).DependOn(cmj, caj, cpj, callj).SetName("pmij")


	job.Run()
	fmt.Println(countAllMapper, pmiMapper, pmiReducer, callj, cmj, caj, cpj)
}
