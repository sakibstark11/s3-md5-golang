package src

import "flag"

type cliArgs struct {
	Bucket    string
	Key       string
	ChunkSize int64
}

func ParseCliArgs() cliArgs {
	bucket := flag.String("bucket", "", "name of the bucket")
	key := flag.String("key", "", "name of the object")
	chunkSize := flag.Int64("chunkSize", 1000000, "chunk size to download on each request")
	flag.Parse()
	return cliArgs{
		Bucket:    *bucket,
		Key:       *key,
		ChunkSize: *chunkSize,
	}
}
