package bucket

import (
	"bytes"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// AliBlobBucket use azure blob
type AliBlobBucket struct {
	bucket     *oss.Bucket
	bucketName string
}

type AliBlobObjectReader struct {
	ObjectReader
	reader io.ReadCloser
}

func (reader *AliBlobObjectReader) Close() error {
	return reader.reader.Close()
}

func (reader *AliBlobObjectReader) Read(p []byte) (int, error) {
	return reader.reader.Read(p)
}

type AliBlobObjectWriter struct {
	ObjectWriter
	bucket  *oss.Bucket
	name    string
	content []byte
}

func (writer *AliBlobObjectWriter) Close() error {
	var err error
	for retry := 0; retry < 3; retry++ {
		err = writer.bucket.PutObject(writer.name, bytes.NewReader(writer.content))
		if err == nil {
			break
		}
	}
	return err
}

func (writer *AliBlobObjectWriter) Write(data []byte) (int, error) {
	writer.content = append(writer.content, data...)
	return len(data), nil
}

// NewAliBlobBucket new ali blob bucket
func NewAliBlobBucket(endpoint, accessKeyId, accessKeySecret string, bucketName string) (Bucket, error) {
	client, err := oss.New(endpoint, accessKeyId, accessKeySecret)
	if err != nil {
		return nil, err
	}
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}
	return &AliBlobBucket{
		bucket:     bucket,
		bucketName: bucketName,
	}, nil
}

func (bk *AliBlobBucket) OpenRead(name string) (ObjectReader, error) {
	ioReader, err := bk.bucket.GetObject(name)
	if err != nil {
		return nil, err
	}
	return &AliBlobObjectReader{
		reader: ioReader,
	}, nil
}

func (bk *AliBlobBucket) OpenWrite(name string) (ObjectWriter, error) {
	return &AliBlobObjectWriter{
		bucket:  bk.bucket,
		name:    name,
		content: make([]byte, 0),
	}, nil
}

func (bk *AliBlobBucket) Delete(key string) error {
	return bk.bucket.DeleteObject(key)
}

func (bk *AliBlobBucket) ListFiles() ([]string, error) {
	res := make([]string, 0)

	marker := oss.Marker("")
	for {
		lor, err := bk.bucket.ListObjects(oss.MaxKeys(10), marker)
		if err != nil {
			panic(err)
		}
		for _, o := range lor.Objects {
			res = append(res, o.Key)
		}
		marker = oss.Marker(lor.NextMarker)
		if !lor.IsTruncated {
			break
		}
	}
	return res, nil
}
