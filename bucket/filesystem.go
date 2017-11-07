package bucket

import (
	"errors"
	"log"
	"os"
	"path/filepath"
)

// FSBucket use a directory as pool
type FSBucket struct {
	directory string
}

// FSObjectReader FSObjectReader
type FSObjectReader struct {
	ObjectReader
	file *os.File
}

// Close close reader
func (reader FSObjectReader) Close() error {
	return reader.file.Close()
}

// Read close reader
func (reader FSObjectReader) Read(p []byte) (n int, err error) {
	return reader.file.Read(p)
}

// FSObjectWriter FileRecordWriter
type FSObjectWriter struct {
	ObjectWriter
	file *os.File
}

// Close Close writer
func (writer FSObjectWriter) Close() error {
	return writer.file.Close()
}

func (writer FSObjectWriter) Write(data []byte) (int, error) {
	return writer.file.Write(data)
}

// NewFSBucket NewFSBucket
func NewFSBucket(directory string) (bk Bucket, err error) {
	if _, err = os.Stat(directory); os.IsNotExist(err) {
		err = os.MkdirAll(directory, 0755)
		if err != nil {
			return
		}
	}
	return FSBucket{directory: directory}, nil
}

// OpenRead Open a RecordReader by name
func (fsb FSBucket) OpenRead(key string) (rd ObjectReader, err error) {
	path := filepath.Join(fsb.directory, key)
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		log.Printf("Fail to open %v for read(abspath: \"%v\"): %v", key, path, err)
	}
	return &FSObjectReader{file: file}, nil
}

// OpenWrite Open a RecordWriter by name
func (fsb FSBucket) OpenWrite(key string) (wr ObjectWriter, err error) {
	var writer FSObjectWriter
	path := filepath.Join(fsb.directory, key)
	writer.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("Fail to open %v for write (abspath: \"%v\"): %v", key, path, err)
	}
	return &writer, nil
}

// Delete Delete object in bucket
func (fsb FSBucket) Delete(key string) error {
	return os.Remove(filepath.Join(fsb.directory, key))
}
func (fsb FSBucket) ListFiles() ([]string, error) {
	return nil, errors.New("fs list file is not implemented")
}
