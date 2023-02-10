package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var bucket *string = flag.String("bucket", "", "name of the bucket")
var key *string = flag.String("key", "", "name of the object")
var chunkSize *int64 = flag.Int64("chunkSize", 1000000, "chunk size to download on each request")

func getObjectSize(client *s3.Client, bucket string, key string) int64 {
	headObjectOutput, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatal(err)
	}
	return headObjectOutput.ContentLength
}

func calculateObjectRange(partNumber int, chunkSize int64, chunkCount int, objectSize int64) string {
	startRange := int64(partNumber * int(chunkSize))
	endRange := objectSize
	if (partNumber + 1) != chunkCount {
		endRange = int64((startRange + chunkSize) - 1)
	}
	return fmt.Sprintf("bytes=%v-%v", startRange, endRange)
}

func main() {
	flag.Parse()
	if *bucket == "" || *key == "" {
		log.Fatal("bucket and key name must be provided. Use -h for help")
	}

	start := time.Now()

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	client := s3.NewFromConfig(cfg)

	objectSize := getObjectSize(client, *bucket, *key)
	if objectSize < *chunkSize {
		log.Fatal("object size must be greater than chunk size")
	}

	log.Printf("object size %v bytes", objectSize)
	chunkCount := int(objectSize / *chunkSize)
	log.Printf("chunk count %v", chunkCount)

	md5Hash := md5.New()
	wg := sync.WaitGroup{}
	results := make([][]byte, chunkCount)

	for partNumber := 0; partNumber < chunkCount; partNumber++ {
		rangeToGet := calculateObjectRange(partNumber, *chunkSize, chunkCount, objectSize)
		wg.Add(1)
		go func(partNumber int, rangeString string) {
			body, err := io.ReadAll(getS3ObjectRange(client, *bucket, *key, rangeString))
			if err != nil {
				log.Fatal(err)
			}

			results[partNumber] = body
			wg.Done()
		}(partNumber, rangeToGet)
	}
	wg.Wait()

	for _, body := range results {
		_, err = md5Hash.Write(body)
		if err != nil {
			log.Fatal(err)
		}
	}
	md5String := hex.EncodeToString(md5Hash.Sum(nil))
	log.Printf("md5 hash %v", md5String)
	elapsed := time.Since(start).Seconds()
	log.Printf("time %v seconds taken", elapsed)
}

func getS3ObjectRange(client *s3.Client, bucket string, key, rangeToGet string) io.ReadCloser {
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
