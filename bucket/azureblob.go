package bucket

import (
	"encoding/base64"
	"errors"
	"io"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// max block size for azure blob
const maxBlockSize = 50 * 1024 * 1024

// AzureBlobBucket use azure blob
type AzureBlobBucket struct {
	container      *storage.Container
	blobNamePrefix string
}

type AzureBlobObjectReader struct {
	ObjectReader
	reader io.ReadCloser
}

func (reader *AzureBlobObjectReader) Close() error {
	return reader.reader.Close()
}

func (reader *AzureBlobObjectReader) Read(p []byte) (n int, err error) {
	return reader.reader.Read(p)
}

type AzureBlobObjectWriter struct {
	ObjectWriter
	blob      *storage.Blob
	name      string
	content   []byte
	idGen     int
	blockList []storage.Block
}

func (writer *AzureBlobObjectWriter) Close() error {
	if len(writer.content) > 0 {
		blockId := base64.StdEncoding.EncodeToString([]byte(writer.name + "_" + strconv.Itoa(writer.idGen)))
		err := writer.blob.PutBlock(blockId, writer.content, &storage.PutBlockOptions{})
		if err != nil {
			return err
		}
		writer.blockList = append(writer.blockList,
			storage.Block{ID: blockId, Status: storage.BlockStatusUncommitted})
	}
	err := writer.blob.PutBlockList(writer.blockList, &storage.PutBlockListOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (writer *AzureBlobObjectWriter) Write(data []byte) (int, error) {
	writer.content = append(writer.content, data...)
	if len(writer.content) >= maxBlockSize {
		blockId := base64.StdEncoding.EncodeToString([]byte(writer.name + "_" + strconv.Itoa(writer.idGen)))
		writer.idGen += 1
		err := writer.blob.PutBlock(blockId, writer.content[:maxBlockSize], &storage.PutBlockOptions{})
		if err != nil {
			return 0, err
		}
		writer.blockList = append(writer.blockList,
			storage.Block{ID: blockId, Status: storage.BlockStatusUncommitted})
		writer.content = writer.content[maxBlockSize:]
	}
	return len(data), nil
}

// NewAzureBlobBucket new azure blob bucket
func NewAzureBlobBucket(accountName, accountKey, containerName, blobServiceBaseUrl, apiVersion string,
	useHttps bool, blobNamePrefix string) (Bucket, error) {
	client, err := storage.NewClient(accountName, accountKey,
		blobServiceBaseUrl, apiVersion, useHttps)
	if err != nil {
		return nil, err
	}
	b := client.GetBlobService()
	return &AzureBlobBucket{
		container:      b.GetContainerReference(containerName),
		blobNamePrefix: blobNamePrefix,
	}, nil
}

func (bk *AzureBlobBucket) OpenRead(name string) (ObjectReader, error) {
	blob := bk.container.GetBlobReference(bk.blobNamePrefix + "/" + name)
	ioReader, err := blob.Get(&storage.GetBlobOptions{})
	if err != nil {
		return nil, err
	}
	return &AzureBlobObjectReader{
		reader: ioReader,
	}, nil
}

func (bk *AzureBlobBucket) OpenWrite(name string) (ObjectWriter, error) {
	blob := bk.container.GetBlobReference(bk.blobNamePrefix + "/" + name)
	isExist, err := blob.Exists()
	if err != nil {
		return nil, err
	}
	if !isExist {
		blob.CreateBlockBlob(&storage.PutBlobOptions{})
	}
	return &AzureBlobObjectWriter{
		blob:      blob,
		name:      name,
		content:   make([]byte, 0),
		idGen:     0,
		blockList: make([]storage.Block, 0),
	}, nil
}

func (bk *AzureBlobBucket) Delete(key string) error {
	blob := bk.container.GetBlobReference(key)
	_, err := blob.DeleteIfExists(&storage.DeleteBlobOptions{})
	return err
}

func (bk *AzureBlobBucket) ListFiles() ([]string, error) {
	return nil, errors.New("azure list file is not implemented")
}
