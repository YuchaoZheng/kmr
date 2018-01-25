// +build !linux

// Package bucket
// This file allow non-linux user to compile KMR on their device
// Attention: do not remove the empty line under the first line of this file

package bucket

import (
	"fmt"

	"github.com/naturali/kmr/util/log"
)

var errNotImplemented = fmt.Errorf("RadosBucket is not implemented on non-linux platform")

// RadosBucket RadosBucket
type RadosBucket struct {
	Bucket
	conn   interface{}
	ioctx  interface{}
	pool   string
	prefix string
}

// RadosObjectReader RadosObjectReader
type RadosObjectReader struct {
	ObjectReader
	bucket *RadosBucket
	name   string
	offset uint64
}

// RadosObjectWriter RadosObjectWriter
type RadosObjectWriter struct {
	ObjectReader
	bucket *RadosBucket
	name   string
	offset uint64
}

// Close close reader
func (reader *RadosObjectReader) Close() error {
	return errNotImplemented
}

// Read close reader
func (reader *RadosObjectReader) Read(p []byte) (n int, err error) {
	return 0, errNotImplemented
}

// Close close writer
func (writer *RadosObjectWriter) Close() error {
	return errNotImplemented
}

// Write Write
func (writer *RadosObjectWriter) Write(data []byte) (int, error) {
	return 0, errNotImplemented
}

// NewRadosBucket NewRadosBucket
func NewRadosBucket(mons, secret, pool, prefix string) (bk Bucket, err error) {
	return nil, errNotImplemented
}

// OpenRead Open a RecordReader by name
func (bk *RadosBucket) OpenRead(key string) (rd ObjectReader, err error) {
	return nil, errNotImplemented
}

// OpenWrite Open a RecordWriter by name
func (bk *RadosBucket) OpenWrite(key string) (wr ObjectWriter, err error) {
	return nil, errNotImplemented
}

// Delete Delete object in bucket
func (bk *RadosBucket) Delete(key string) error {
	return errNotImplemented
}

func (bk *RadosBucket) CreateDir(files []string) error {
	return errNotImplemented
}

func (bk *RadosBucket) GetFilePath(key string) string {
	log.Fatal("RadosBucket can't use GetFilePath")
	return ""
}
