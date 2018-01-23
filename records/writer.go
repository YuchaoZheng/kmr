package records

import (
	"bufio"
	"os"

	"github.com/naturali/kmr/bucket"
	"github.com/syndtr/goleveldb/leveldb"
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
	if err != nil {
		panic("fail to put Record to leveldb")
	}
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
	switch name {
	case "file":
		return NewFileRecordWriter(params["filename"].(string))
	case "console":
		return NewConsoleRecordWriter()
	case "stream":
		return NewStreamRecordWriter(params["writer"].(bucket.ObjectWriter))
	case "leveldb":
		return NewLeveldbRecordWriter(params["filename"].(string))
	default:
		return NewConsoleRecordWriter()
	}
}
