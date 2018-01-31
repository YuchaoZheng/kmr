package records

import (
	"bufio"
	"compress/bzip2"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/util/log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/ryszard/tfutils/go/tfrecord"
)

type RecordReader interface {
	Peek() *Record // get first record without drop it
	Pop() *Record
	HasNext() bool
	Close() error
}

type SimpleRecordReader struct {
	input  <-chan *Record
	first  *Record
	reader bucket.ObjectReader
}

func NewSimpleRecordReader(input <-chan *Record) *SimpleRecordReader {
	reader := SimpleRecordReader{
		input: input,
		first: nil,
	}
	return &reader
}

func (srr *SimpleRecordReader) Peek() *Record {
	return srr.first
}

func (srr *SimpleRecordReader) Pop() (res *Record) {
	res = srr.first
	srr.first = nil
	return
}

func (srr *SimpleRecordReader) HasNext() bool {
	if srr.first != nil {
		return true
	}
	record, ok := <-srr.input
	if !ok {
		return false
	}
	srr.first = record
	return true
}

func (srr *SimpleRecordReader) Close() error {
	if srr.reader != nil {
		return srr.reader.Close()
	}
	return nil
}

func NewConsoleRecordReader() *SimpleRecordReader {
	reader := bufio.NewReader(os.Stdin)
	preload := make(chan *Record, 1000)

	feedStream(preload, reader)

	return &SimpleRecordReader{
		input: preload,
	}
}

func NewFileRecordReader(filename string) *SimpleRecordReader {
	file, err := os.Open(filename)
	if err != nil {
		panic("fail to create file reader")
	}
	reader := bufio.NewReader(file)
	preload := make(chan *Record, 1000)

	feedStream(preload, reader)

	return &SimpleRecordReader{
		input: preload,
	}
}

func NewStreamRecordReader(reader bucket.ObjectReader) *SimpleRecordReader {

	preload := make(chan *Record, 1000)

	feedStream(preload, reader)

	return &SimpleRecordReader{
		input:  preload,
		reader: reader,
	}
}

func NewTextStreamRecordReader(reader bucket.ObjectReader) *SimpleRecordReader {
	preload := make(chan *Record, 1000)

	feedTextStream(preload, reader)

	return &SimpleRecordReader{
		input:  preload,
		reader: reader,
	}
}

func NewTextFileRecordReader(filename string) *SimpleRecordReader {
	file, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("fail to create file reader of '%s', %v", filename, err))
	}
	reader := bufio.NewReader(file)
	preload := make(chan *Record, 1000)

	feedTextStream(preload, reader)

	return &SimpleRecordReader{
		input:  preload,
		reader: file,
	}
}

func NewBz2RecordReader(reader bucket.ObjectReader) *SimpleRecordReader {
	bzreader := bzip2.NewReader(bufio.NewReader(reader))
	preload := make(chan *Record, 1000)
	feedTextStream(preload, bzreader)

	return &SimpleRecordReader{
		input:  preload,
		reader: reader,
	}
}

func NewMemoryRecordReader(records []*Record) *SimpleRecordReader {
	preload := make(chan *Record, 1000)
	go func() {
		for _, r := range records {
			preload <- r
		}
		close(preload)
	}()
	return &SimpleRecordReader{
		input: preload,
	}
}

func NewReadAllBytesReader(reader bucket.ObjectReader) *SimpleRecordReader {
	preload := make(chan *Record, 1)
	go func() {
		b, err := ioutil.ReadAll(reader)
		if err != nil {
			panic(fmt.Sprintf("fail to ReadAll from reader %v", err))
		}
		preload <- &Record{Key: make([]byte, 4), Value: b}
		close(preload)
	}()
	return &SimpleRecordReader{
		input:  preload,
		reader: reader,
	}
}

func NewLevelDbRecordReader(filename string) *SimpleRecordReader{
	db, err := leveldb.OpenFile(filename, nil)
	if err != nil {
		panic("fail to create file reader")
	}
	preload := make(chan *Record, 1000)

	go func() {
		iter := db.NewIterator(nil, nil)
		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			keyb := make([]byte, len(key))
			valueb := make([]byte, len(value))
			copy(keyb, key)
			copy(valueb, value)
			preload <- &Record{Key: keyb, Value: valueb}
		}
		iter.Release()
		err := iter.Error()
		if err != nil {
			log.Fatal(err)
		}
		close(preload)
		db.Close()
	} ()

	return &SimpleRecordReader{
		input: preload,
	}
}

func NewTfRecordReader(reader bucket.ObjectReader) *SimpleRecordReader{
	preload := make(chan *Record, 1000)
	feedTfRecord(preload, reader)

	return &SimpleRecordReader{
		input:  preload,
		reader: reader,
	}
}

func feedTfRecord(preload chan<- *Record, reader io.Reader) {
	go func() {
		for {
			value, err := tfrecord.Read(reader)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			preload <- &Record{Key: make([]byte, 4), Value: value}
		}
		close(preload)
	}()
}

func feedStream(preload chan<- *Record, reader io.Reader) {
	go func() {
		r := bufio.NewReaderSize(reader, 1024*1024)
		for {
			var err error
			// Read Key
			record, err := ReadRecord(r)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			preload <- record
		}
		close(preload)
	}()
}

// feedTextStream read text file, emit (linenumber.(uint32), line.([]byte))
func feedTextStream(preload chan<- *Record, reader io.Reader) {
	go func() {
		r := bufio.NewReaderSize(reader, 1024*1024)
		var lineNum uint32
		for {
			line, err := r.ReadBytes('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			record := &Record{Key: make([]byte, 4), Value: line}
			binary.BigEndian.PutUint32(record.Key, lineNum)
			preload <- record
			lineNum++
		}
		close(preload)
	}()
}

func MakeRecordReader(name string, params map[string]interface{}) RecordReader {
	// TODO: registry
	// noway to instance directly by type name in Golang
	var reader bucket.ObjectReader
	var err error
	if name == "bz2" || name == "stream" || name == "textstream" || name == "readAllBytes" || name == "tfrecord" {
		reader, err = params["bucket"].(bucket.Bucket).OpenRead(params["filename"].(string))
		if err != nil {
			log.Errorf("Fail to open object %s: %v", params["filename"].(string), err)
		}
	}
	switch name {
	case "textfile":
		return NewTextFileRecordReader(params["filename"].(string))
	case "bz2":
		return NewBz2RecordReader(reader)
	case "file":
		return NewFileRecordReader(params["filename"].(string))
	case "stream":
		return NewStreamRecordReader(reader)
	case "textstream":
		return NewTextStreamRecordReader(reader)
	case "memory":
		return NewMemoryRecordReader(params["data"].([]*Record))
	case "console":
		return NewConsoleRecordReader()
	case "readAllBytes":
		return NewReadAllBytesReader(reader)
	case "leveldb":
		filename := params["bucket"].(bucket.Bucket).GetFilePath(params["filename"].(string))
		return NewLevelDbRecordReader(filename)
	case "tfrecord":
		return NewTfRecordReader(reader)
	default:
		log.Debugf("Warning, ReaderType = \"%s\", you are using default reader(NewConsoleRecordReader).", name)
		return NewConsoleRecordReader()
	}
}
