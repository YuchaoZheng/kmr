
// +build linux

package bucket

import (
	"errors"
	"io"
	"log"

	"github.com/ceph/go-ceph/rados"
)

// RadosBucket RadosBucket
type RadosBucket struct {
	Bucket
	conn   *rados.Conn
	ioctx  *rados.IOContext
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

// RadosObjectReader RadosObjectReader
type RadosObjectWriter struct {
	ObjectReader
	bucket *RadosBucket
	name   string
	offset uint64
}

// Close close reader
func (reader *RadosObjectReader) Close() error {
	return nil
}

// Read read from object
func (reader *RadosObjectReader) Read(p []byte) (n int, err error) {
	n, err = reader.bucket.ioctx.Read(reader.bucket.prefix+reader.name, p, reader.offset)
	reader.offset += uint64(n)
	// base on our experiment, go-ceph will return 0, nil when reach EOF. We should confirm that later.
	if err == nil && n == 0 {
		return 0, io.EOF
	}
	return n, err
}

// Close close writer
func (writer *RadosObjectWriter) Close() error {
	return nil
}

func (writer *RadosObjectWriter) Write(data []byte) (int, error) {
	err := writer.bucket.ioctx.Write(writer.bucket.prefix+writer.name, data, writer.offset)
	if err != nil {
		return 0, err
	}
	writer.offset += uint64(len(data))
	return len(data), nil

}

func NewRadosBucket(mons, secret, pool, prefix string) (bk Bucket, err error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, err
	}
	conn.SetConfigOption("mon_host", mons)
	conn.SetConfigOption("key", secret)
	err = conn.Connect()
	if err != nil {
		log.Println("Failed to connect ceph:", err)
		return nil, err
	}
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		log.Printf("Cannot open pool %s: %v", pool, err)
		return nil, err
	}
	return &RadosBucket{
		conn:   conn,
		ioctx:  ioctx,
		pool:   pool,
		prefix: prefix,
	}, nil
}

// OpenRead Open a RecordReader by name
func (bk *RadosBucket) OpenRead(key string) (rd ObjectReader, err error) {
	return &RadosObjectReader{
		bucket: bk,
		name:   key,
		offset: uint64(0),
	}, nil
}

// OpenWrite Open a RecordWriter by name
func (bk *RadosBucket) OpenWrite(key string) (wr ObjectWriter, err error) {
	return &RadosObjectWriter{
		bucket: bk,
		name:   key,
		offset: uint64(0),
	}, nil
}

// Delete Delete object in bucket
func (bk *RadosBucket) Delete(key string) error {
	return bk.ioctx.Delete(bk.prefix + key)
}

func (bk *RadosBucket) CreateDir(files []string) error {
	return errors.New("not support CreateDir on RadosBucket")
}

func (bk *RadosBucket) GetFilePath(key string) string {
	log.Fatal("RadosBucket can't use GetFilePath")
	return ""
}
