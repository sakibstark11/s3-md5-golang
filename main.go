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

	if bucket == "" || key == "" {
		log.Fatal("bucket and key name must be provided. Use -h for help")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	chunkSize := args.ChunkSize
	s3Helper := src.S3Helper{
		Bucket:    bucket,
		Key:       key,
		Client:    s3.NewFromConfig(cfg),
		ChunkSize: chunkSize,
	}

	objectSize, err := s3Helper.GetObjectSize()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("object size %v bytes", objectSize)

	chunkCount := int(objectSize / chunkSize)
	log.Printf("chunk count %v", chunkCount)

	md5Hash := md5.New()
	wg := sync.WaitGroup{}
	results := make([][]byte, chunkCount)

	for partNumber := 0; partNumber < chunkCount; partNumber++ {
		rangeToGet := s3Helper.CalculateObjectRange(partNumber, chunkCount)
		wg.Add(1)
		go func(partNumber int, rangeString string) {
			body, err := io.ReadAll(s3Helper.GetS3ObjectRange(rangeString))
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
