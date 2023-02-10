![Gopher](https://go.dev/blog/gopher/header.jpg)

Get fast md5 hashes for an s3 object using multiple gophers (i.e go routines).

# How to use
## Prerequisites
- Go 1.x
### Steps
- Run
    ```shell
    go get .
    ```
    to get all the dependencies
- Run
    ```shell
    go run main.go --bucket <bucket name> --key <key name> --chunkSize <chunk size>
    ```
    where
    - **bucket**: name of the bucket
    - **key**: name of the key/file
    - **chunkSize**(optional, default `1000000 bytes`): amount of data in bytes to download in each request. Must be less than object size
