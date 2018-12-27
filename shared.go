package main

import (
	"bytes"
	"os"

	"github.com/marcoalmeida/pgCarpenter/util"
	"go.uber.org/zap"
)

// upload path to the S3 bucket using key as the object name; stat is the FileInfo for the original
// file being uploaded (path might be a compressed version)
func (a *app) upload(path string, key string, mtime int64) error {
	// open the compressed file to upload
	file, err := os.Open(path)
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

	a.logger.Debug("Uploading file", zap.String("key", key), zap.String("path", path))
	if size > 5*1024*1024 {
		_, err = a.s3Uploader.Upload(util.GetUploadInput(a.s3Bucket, &key, body, mtime))
	} else {
		_, err = a.s3Client.PutObject(util.GetPutObjectInput(a.s3Bucket, &key, body, mtime))
	}
	if err != nil {
		return err
	}

	return nil
}

// remove a file on a best effort basis -- handle a possible error by logging it
func (a *app) mustRemoveFile(path string) {
	a.logger.Debug("Removing file", zap.String("path", path))
	if err := os.Remove(path); err != nil {
		// there's not a lot we can do here
		a.logger.Error("Failed to remove temporary file", zap.String("path", path), zap.Error(err))
	}
}
