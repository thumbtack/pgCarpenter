package s3storage

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/marcoalmeida/pgCarpenter/storage"
	"go.uber.org/zap"
)

const (
	// using uppercase first letter because that's how the Go SDK will
	// deserialize it and the inconsistency would probably throw us off at some point
	metadataUploadTime   = "Upload_time"
	metadataModifiedTime = "Modified_time"
)

type s3Storage struct {
	client     *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	bucket     string
	logger     *zap.Logger
}

func New(bucket string, region string, maxRetries int, logger *zap.Logger) storage.Storage {
	backend := &s3Storage{bucket: bucket, logger: logger}

	// generic S3 client
	backend.client = s3.New(session.Must(
		session.NewSessionWithOptions(
			session.Options{
				Config: aws.Config{
					Region:                        aws.String(region),
					MaxRetries:                    aws.Int(maxRetries),
					CredentialsChainVerboseErrors: aws.Bool(true)},
				SharedConfigState:       session.SharedConfigEnable,
				AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
			})))

	// the s3 manager is helpful with large file uploads; also thread-safe
	backend.uploader = s3manager.NewUploaderWithClient(backend.client, func(u *s3manager.Uploader) {
		u.PartSize = 32 * 1024 * 1024
		u.Concurrency = 32
		u.LeavePartsOnError = false
	})

	// similarly, this is helpful with large downloads
	backend.downloader = s3manager.NewDownloaderWithClient(backend.client, func(u *s3manager.Downloader) {
		u.PartSize = 32 * 1024 * 1024
		u.Concurrency = 32
	})

	return backend
}

func (s s3Storage) Put(objectKey string, localPath string, mtime int64) error {
	// open the compressed file to upload
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	// read the compressed file into a buffer
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	_, err = file.Read(buffer)
	if err != nil {
		return err
	}
	// prepare the body of the upload
	body := bytes.NewReader(buffer)

	s.logger.Debug("Uploading file", zap.String("objectKey", objectKey), zap.String("localPath", localPath))
	if size > 5*1024*1024 {
		_, err = s.uploader.Upload(getUploadInput(&s.bucket, &objectKey, body, mtime))
	} else {
		_, err = s.client.PutObject(getPutObjectInput(&s.bucket, &objectKey, body, mtime))
	}
	if err != nil {
		return err
	}

	return nil
}

func (s s3Storage) PutString(key string, body string) error {
	s.logger.Debug("Creating object", zap.String("key", key))

	_, err := s.client.PutObject(getPutObjectInput(&s.bucket, &key, strings.NewReader(body), time.Now().Unix()))
	if err != nil {
		return err
	}

	return nil
}

func (s s3Storage) Get(key string, out io.WriterAt) error {
	_, err := s.downloader.Download(
		out,
		&s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return err
	}

	return nil
}

func (s s3Storage) GetString(key string) (string, error) {
	result, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}

	defer result.Body.Close()

	// read the file
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, result.Body); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (s s3Storage) GetLastModifiedTime(key string) (int64, error) {
	result, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}

	mtime, ok := result.Metadata[metadataModifiedTime]
	if ok {
		mtime, err := strconv.Atoi(*mtime)
		if err != nil {
			return 0, err
		}

		return int64(mtime), nil
	}

	return 0, nil
}

func (s s3Storage) ListFolder(path string) ([]string, error) {
	keys := make([]string, 0)

	var next *string = nil
	for {
		input := &s3.ListObjectsV2Input{
			Bucket:    aws.String(s.bucket),
			Delimiter: aws.String("/"),
			Prefix:    aws.String(path),
		}

		// include the continuation token, if there's one
		if next != nil {
			input.ContinuationToken = next
		}
		result, err := s.client.ListObjectsV2(input)
		if err != nil {
			return nil, err
		}

		// collect all keys
		for _, prefix := range result.CommonPrefixes {
			keys = append(keys, *prefix.Prefix)
		}

		if *result.IsTruncated {
			next = result.NextContinuationToken
		} else {
			return keys, nil
		}

	}
}

func (s s3Storage) WalkFolder(path string, keysC chan<- string) error {
	var next *string = nil
	for {
		input := &s3.ListObjectsV2Input{
			Bucket:    aws.String(s.bucket),
			Delimiter: aws.String("/"),
			Prefix:    aws.String(path),
		}
		// include the continuation token, if there's one
		if next != nil {
			input.ContinuationToken = next
		}
		result, err := s.client.ListObjectsV2(input)
		if err != nil {
			return err
		}

		// objects to restore
		for _, obj := range result.Contents {
			s.logger.Debug("Found object while traversing folder", zap.String("key", *obj.Key))
			if *obj.Key == path {
				s.logger.Debug("Skipping folder", zap.String("path", *obj.Key))
				continue
			}
			keysC <- *obj.Key
		}

		// child folders to process
		for _, p := range result.CommonPrefixes {
			s.logger.Debug("Processing child folder", zap.String("prefix", *p.Prefix))
			if err := s.WalkFolder(*p.Prefix, keysC); err != nil {
				return err
			}
		}

		if *result.IsTruncated {
			next = result.NextContinuationToken
		} else {
			s.logger.Debug("Done traversing folder", zap.String("prefix", path))
			return nil
		}
	}
}

// return a map with generally useful metadata for Put/Upload operations
func generateS3ObjectMetadata(mtime int64) map[string]*string {
	now := strconv.FormatInt(time.Now().Unix(), 10)

	metadata := map[string]*string{
		metadataUploadTime: aws.String(now),
	}

	// add file size and modified timestamp, if provided
	if mtime != 0 {
		metadata[metadataModifiedTime] = aws.String(strconv.FormatInt(mtime, 10))
	}

	return metadata
}

// getPutObjectInput creates and returns a pointer to an instance of s3.PutObjectInput that includes
// the object's metadata as required and used by pgCarpenter.
func getPutObjectInput(bucket *string, key *string, body io.ReadSeeker, mtime int64) *s3.PutObjectInput {
	return &s3.PutObjectInput{
		Bucket:   bucket,
		Key:      key,
		Body:     body,
		Metadata: generateS3ObjectMetadata(mtime),
	}
}

// getUploadInput creates and returns a pointer to an instance of s3manager.UploadInput that includes
// the object's metadata as required and used by pgCarpenter
func getUploadInput(bucket *string, key *string, body io.Reader, mtime int64) *s3manager.UploadInput {
	return &s3manager.UploadInput{
		Bucket:   bucket,
		Key:      key,
		Body:     body,
		Metadata: generateS3ObjectMetadata(mtime),
	}
}
