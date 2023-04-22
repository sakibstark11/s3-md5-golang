package src

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Helper struct {
	Client     *s3.Client
	Bucket     string
	Key        string
	objectSize int64
	ChunkSize  int64
}

func (helper *S3Helper) GetObjectSize() (int64, error) {
	headObjectOutput, err := helper.Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &helper.Bucket,
		Key:    &helper.Key,
	})
	if err != nil {
		log.Fatal(err)
	}
	objectSize := headObjectOutput.ContentLength
	if objectSize < helper.ChunkSize {

		return 0, errors.New("object size must be greater than chunk size")
	}
	helper.objectSize = objectSize

	return objectSize, nil
}

func (helper *S3Helper) CalculateObjectRange(partNumber int, chunkCount int) string {
	startRange := int64(partNumber) * helper.ChunkSize
	endRange := helper.objectSize
	if (partNumber + 1) != chunkCount {
		endRange = (startRange + helper.ChunkSize) - 1
	}

	return fmt.Sprintf("bytes=%v-%v", startRange, endRange)
}

func (helper *S3Helper) GetS3ObjectRange(rangeToGet string) io.ReadCloser {
	getObjectOutput, err := helper.Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &helper.Bucket,
		Key:    &helper.Key,
		Range:  &rangeToGet,
	})
	if err != nil {
		log.Fatal(err)
	}
	return getObjectOutput.Body
}
