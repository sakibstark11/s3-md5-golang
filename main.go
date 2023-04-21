package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"log"
	"s3-md5-golang/src"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	start := time.Now()

	args := src.ParseCliArgs()
	bucket := args.Bucket
	key := args.Key
	chunkSize := args.ChunkSize
	if bucket == "" || key == "" {
		log.Fatal("bucket and key name must be provided. Use -h for help")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	client := s3.NewFromConfig(cfg)

	objectSize := src.GetObjectSize(client, bucket, key)
	if objectSize < chunkSize {
		log.Fatal("object size must be greater than chunk size")
	}

	log.Printf("object size %v bytes", objectSize)
	chunkCount := int(objectSize / chunkSize)
	log.Printf("chunk count %v", chunkCount)

	md5Hash := md5.New()
	wg := sync.WaitGroup{}
	results := make([][]byte, chunkCount)

	for partNumber := 0; partNumber < chunkCount; partNumber++ {
		rangeToGet := src.CalculateObjectRange(partNumber, chunkSize, chunkCount, objectSize)
		wg.Add(1)
		go func(partNumber int, rangeString string) {
			body, err := io.ReadAll(src.GetS3ObjectRange(client, bucket, key, rangeString))
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
