package src

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"sync"
)

func ProcessMD5(s3Helper *S3Helper, chunkSize int64) (string, error) {

	objectSize, err := s3Helper.GetObjectSize()
	if err != nil {
		return "", err
	}

	log.Printf("object size %v bytes", objectSize)
	chunkCount := int(objectSize / chunkSize)
	log.Printf("chunk count %v", chunkCount)

	md5Hash := md5.New()
	wg := sync.WaitGroup{}
	results := make([][]byte, chunkCount)
	errorsChannel := make(chan error)

	continueSignalChannel := make(chan struct{})
	for partNumber := 0; partNumber < chunkCount; partNumber++ {
		rangeToGet := s3Helper.CalculateObjectRange(partNumber, chunkCount)
		go func(partNumber int, rangeString string, continueSignal chan struct{}, errorsChannel chan error) {
			defer wg.Done()

			log.Printf("partNumber %v", partNumber)

			if _, open := <-continueSignal; !open {
				log.Printf("cancelling signal received, cancelling %v request", partNumber)
				return
			}

			body, err := io.ReadAll(s3Helper.GetS3ObjectRange(rangeString))

			if err != nil {
				errorsChannel <- fmt.Errorf("partNumber %v failed, %w", partNumber, err)
				close(errorsChannel)
				close(continueSignalChannel)
				return
			}

			results[partNumber] = body
		}(partNumber, rangeToGet, continueSignalChannel, errorsChannel)
		wg.Add(1)
	}
	wg.Wait()

	// if err := <-errorsChannel; err != nil {
	// 	log.Printf("one of the requests failed, check logs")
	// 	return "", err
	// }

	for _, body := range results {
		_, err = md5Hash.Write(body)
		if err != nil {
			return "", err
		}
	}
	md5String := hex.EncodeToString(md5Hash.Sum(nil))
	// defer close(errorsChannel)
	// defer close(continueSignalChannel)

	return md5String, nil
}
