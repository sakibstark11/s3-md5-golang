package src

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func GetObjectSize(client *s3.Client, bucket string, key string) int64 {
	headObjectOutput, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatal(err)
	}
	return headObjectOutput.ContentLength
}

func CalculateObjectRange(partNumber int, chunkSize int64, chunkCount int, objectSize int64) string {
	startRange := int64(partNumber * int(chunkSize))
	endRange := objectSize
	if (partNumber + 1) != chunkCount {
		endRange = int64((startRange + chunkSize) - 1)
	}
	return fmt.Sprintf("bytes=%v-%v", startRange, endRange)
}

func GetS3ObjectRange(client *s3.Client, bucket string, key, rangeToGet string) io.ReadCloser {
	log.Printf("range %v fetching", rangeToGet)
	getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Range:  &rangeToGet,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("range %v fetched", rangeToGet)
	return getObjectOutput.Body
}
