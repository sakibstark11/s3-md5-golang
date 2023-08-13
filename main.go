package main

import (
	"context"
	"log"
	"s3-md5-golang/src"
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

	md5String, err := src.ProcessMD5(&s3Helper, chunkSize)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("md5 hash %v", md5String)
	elapsed := time.Since(start).Seconds()
	log.Printf("time %v seconds taken", elapsed)
}
