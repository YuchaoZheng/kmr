package records

import (
	"bufio"
	"os"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/util/log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/ryszard/tfutils/go/tfrecord"
)

type RecordWriter interface {
	Write([]byte) (int, error) // raw Write
	Flush() error              // Flush
	WriteRecord(*Record) error
	Close() error
}

type SimpleRecordWriter struct {
	writer    bucket.ObjectWriter
	bufWriter *bufio.Writer
}

func NewConsoleRecordWriter() *SimpleRecordWriter {
	return &SimpleRecordWriter{
		writer: os.Stdout,
	}
}

func (srw *SimpleRecordWriter) Write(p []byte) (int, error) {
	return srw.bufWriter.Write(p)
}

func (srw *SimpleRecordWriter) Flush() error {
	return srw.bufWriter.Flush()
}

func (srw *SimpleRecordWriter) Close() error {
	srw.Flush()
	return srw.writer.Close()
}

func (srw *SimpleRecordWriter) WriteRecord(record *Record) error {
	return WriteRecord(srw.bufWriter, record)
}

func NewFileRecordWriter(filename string) *SimpleRecordWriter {
	// TODO:
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0766)
	if err != nil {
		panic("fail to create file reader")
	}

	return &SimpleRecordWriter{
		writer:    file,
		bufWriter: bufio.NewWriter(file),
	}
}

func NewStreamRecordWriter(writer bucket.ObjectWriter) *SimpleRecordWriter {

	return &SimpleRecordWriter{
		writer:    writer,
		bufWriter: bufio.NewWriterSize(writer, 4*1024*1024),
	}
}

type TfRecordRecordWriter struct {
	writer bucket.ObjectWriter
}

func NewTfRecordRecordWriter(writer bucket.ObjectWriter) *TfRecordRecordWriter {
	return &TfRecordRecordWriter{
		writer: writer,
	}
}

func (trw *TfRecordRecordWriter) WriteRecord(record *Record) error {
	err := tfrecord.Write(trw.writer, record.Key)
	if err != nil {
		return err
	}
	err = tfrecord.Write(trw.writer, record.Value)
	return err
}

func (trw *TfRecordRecordWriter) Write(p []byte) (int, error) {
	panic("TfRecordRecordWriter must not call func Write")
	return 0, nil
}

func (trw *TfRecordRecordWriter) Flush() error {
	panic("TfRecordRecordWriter must not call func Flush")
	return nil
}

func (trw *TfRecordRecordWriter) Close() error {
	return trw.writer.Close()
}

type LeveldbRecordWriter struct {
	db *leveldb.DB
}

func NewLeveldbRecordWriter(filename string) *LeveldbRecordWriter {
	db, err := leveldb.OpenFile(filename, nil)
	if err != nil {
		panic("fail to create file reader")
	}
	return &LeveldbRecordWriter{db}
}

func (lrw *LeveldbRecordWriter) WriteRecord(record *Record) error {
	err := lrw.db.Put(record.Key, record.Value, nil)
	return err
}

func (lrw *LeveldbRecordWriter) Write(p []byte) (int, error) {
	panic("LeveldbRecordWriter must not call func Write")
	return 0, nil
}

func (lrw *LeveldbRecordWriter) Flush() error {
	panic("LeveldbRecordWriter must not call func Flush")
	return nil
}

func (lrw *LeveldbRecordWriter) Close() error {
	return lrw.db.Close()
}

func MakeRecordWriter(name string, params map[string]interface{}) RecordWriter {
	// TODO: registry
	// noway to instance directly by type name in Golang
	var writer bucket.ObjectWriter
	var err error
	if name == "stream" || name == "tfrecord" {
		writer, err = params["bucket"].(bucket.Bucket).OpenWrite(params["filename"].(string))
		if err != nil {
			log.Errorf("Fail to open object %s: %v", params["filename"].(string), err)
		}
	}
	switch name {
	case "file":
		return NewFileRecordWriter(params["filename"].(string))
	case "console":
		return NewConsoleRecordWriter()
	case "stream":
		return NewStreamRecordWriter(writer)
	case "leveldb":
		filename := params["bucket"].(bucket.Bucket).GetFilePath(params["filename"].(string))
		return NewLeveldbRecordWriter(filename)
	case "tfrecord":
		return NewTfRecordRecordWriter(writer)
	default:
		return NewConsoleRecordWriter()
	}
}
