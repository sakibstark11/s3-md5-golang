package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var bucket string = "s3-md5-bucket"
var key string = "test.jpg"

func main() {
	start := time.Now()

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	client := s3.NewFromConfig(cfg)

	headObjectOutput, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatal(err)
	}
	objectSize := headObjectOutput.ContentLength
	var chunkSize int64 = 1000000
	var chunkCount int = int(objectSize / chunkSize)

	md5Hash := md5.New()
	wg := sync.WaitGroup{}
	results := make([][]byte, chunkCount)

	for i := 0; i < chunkCount; i++ {
		startRange := int64(i * int(chunkSize)) // 0
		var endRange int64
		if (i + 1) == chunkCount {
			endRange = objectSize
		} else {
			endRange = int64((startRange + chunkSize) - 1)
		}
		rangeToGet := fmt.Sprintf("bytes=%v-%v", startRange, endRange)

		wg.Add(1)
		go func(partNumber int, rangeString string) {
			body, err := io.ReadAll(getS3ObjectRange(client, &rangeString))
			if err != nil {
				log.Fatal(err)
			}

			results[partNumber] = body
			wg.Done()
		}(i, rangeToGet)
	}
	wg.Wait()

	for _, body := range results {
		_, err = md5Hash.Write(body)
		if err != nil {
			log.Fatal(err)
		}
	}
	md5String := hex.EncodeToString(md5Hash.Sum(nil))
	fmt.Println(md5String)
	elapsed := time.Since(start).Seconds()
	fmt.Printf("time %v seconds", elapsed)
}

func getS3ObjectRange(client *s3.Client, rangeToGet *string) io.ReadCloser {
	getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Range:  rangeToGet,
	})
	if err != nil {
		log.Fatal(err)
	}

	return getObjectOutput.Body
}
