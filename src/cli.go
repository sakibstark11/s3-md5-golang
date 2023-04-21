package src

import "flag"

var bucket *string = flag.String("bucket", "", "name of the bucket")
var key *string = flag.String("key", "", "name of the object")
var chunkSize *int64 = flag.Int64("chunkSize", 1000000, "chunk size to download on each request")

type cliArgs struct {
	Bucket    string
	Key       string
	ChunkSize int64
}

func ParseCliArgs() cliArgs {
	flag.Parse()
	return cliArgs{
		Bucket:    *bucket,
		Key:       *key,
		ChunkSize: *chunkSize,
	}
}
