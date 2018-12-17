package util

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pierrec/lz4"
)

const (
	// using uppercase first letter because that's how the Go SDK will
	// deserialize it and the inconsistency would probably throw us off at some point
	MetadataUploadTime   = "Upload_time"
	MetadataSize         = "Size"
	MetadataModifiedTime = "Modified_time"
)

// return a map with generally useful metadata for Put/Upload operations
func generateObjectMetadata(info os.FileInfo) map[string]*string {
	now := strconv.FormatInt(time.Now().Unix(), 10)

	metadata := map[string]*string{
		MetadataUploadTime: aws.String(now),
	}

	// add file size and modified timestamp, if provided
	if info != nil {
		metadata[MetadataSize] = aws.String(strconv.FormatInt(info.Size(), 10))
		metadata[MetadataModifiedTime] = aws.String(strconv.FormatInt(info.ModTime().Unix(), 10))
	}

	return metadata
}

// create and return a PutObjectInput instance with useful metadata
func GetPutObjectInput(bucket *string, key *string, body io.ReadSeeker, info os.FileInfo) *s3.PutObjectInput {
	return &s3.PutObjectInput{
		Bucket:   bucket,
		Key:      key,
		Body:     body,
		Metadata: generateObjectMetadata(info),
	}
}

func GetUploadInput(bucket *string, key *string, body io.Reader, info os.FileInfo) *s3manager.UploadInput {
	return &s3manager.UploadInput{
		Bucket:   bucket,
		Key:      key,
		Body:     body,
		Metadata: generateObjectMetadata(info),
	}
}

// return true iff the file is compressed, i.e., the extension indicates it
func IsCompressed(path string) bool {
	return path[len(path)-len(lz4.Extension):] == lz4.Extension
}

func Compress(inPath string, tmp string) (string, error) {
	// create a temporary file with a unique name compress it -- multiple files
	// are named 000: pg_notify/0000, pg_subtrans/0000
	outFile, err := ioutil.TempFile(tmp, "pgCarpenter.")
	if err != nil {
		return "", err
	}

	// open input file
	inFile, err := os.Open(inPath)
	if err != nil {
		return "", err
	}
	// we open this for read only, and this process exists after a finite (short)
	// period of time; there's no need to throw an error if closing it fails
	defer inFile.Close()

	// buffer read from the input file and lz4 writer
	r := bufio.NewReader(inFile)
	w := lz4.NewWriter(outFile)

	// read 4k at a time
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return "", err
		}

		// we're done
		if n == 0 {
			break
		}

		// write the 4k chunk
		if _, err := w.Write(buf[:n]); err != nil {
			return "", err
		}
	}

	// flush any pending compressed data
	if err = w.Flush(); err != nil {
		return "", err
	}

	// make sure we successfully close the compressed file
	if err := outFile.Close(); err != nil {
		return "", err
	}

	return outFile.Name(), nil
}

func Decompress(inPath string, outPath string) error {
	// open the input, compressed file
	inFile, err := os.Open(inPath)
	if err != nil {
		return err
	}
	// we open this for read only, and this process exists after a finite (short)
	// period of time; there's no need to throw an error if closing it fails
	defer inFile.Close()

	// open output file
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}

	// lz4 read buffer
	r := lz4.NewReader(inFile)
	// write buffer
	w := bufio.NewWriter(outFile)

	// 4kb chunks
	buf := make([]byte, 4096)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}

	// flush and pending data
	if err = w.Flush(); err != nil {
		panic(err)
	}

	// make sure we successfully close the compressed file
	if err := outFile.Close(); err != nil {
		return err
	}

	return nil
}
