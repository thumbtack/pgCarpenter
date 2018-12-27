package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akamensky/argparse"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/marcoalmeida/pgCarpenter/util"
	"github.com/pierrec/lz4"
	"go.uber.org/zap"
)

// we don't backup up empty directories, but the ones below must exist in order for PG to start
var directoriesThatMustExist = []string{"pg_tblspc", "pg_replslot", "pg_stat", "pg_snapshots", "pg_xlog"}

func (a *app) restoreBackup() int {
	// create a channel for distributing work
	// spawn nWorkers
	// list all files in backupName, and for each file:
	//   put full path to the S3 object in the channel
	// workers:
	//   download the file to a.pgDataDirectory keeping the relative path
	//   e.g., s3://backupName/base/3456.gz --> a.pgDataDirectory/base/3456.gz
	//   decompress the file: a.pgDataDirectory/base/3456.gz --> a.pgDataDirectory/base/3456
	//
	// keep a counter of total number of files + number of files retrieved
	// print each time if in verbose mode

	// if requested, find the name of the latest backup and update the app struct
	if *a.backupName == latestKey {
		if err := a.resolveLatest(); err != nil {
			a.logger.Error("Failed to resolve the name of the backup for "+latestKey, zap.Error(err))
			return 1
		}
	}

	a.logger.Info("Starting to restore backup", zap.String("name", *a.backupName))
	begin := time.Now()

	// channel to keep the path of all files that need to compressed and uploaded
	restoreFilesC := make(chan string)

	// spawn a pool of workers
	a.logger.Info("Spawning workers", zap.Int("number", *a.nWorkers))
	wg := &sync.WaitGroup{}
	wg.Add(*a.nWorkers)
	for i := 0; i < *a.nWorkers; i++ {
		go a.restoreWorker(restoreFilesC, wg)
	}

	// kick off the (recursive) listing of all folders and restoring of each file
	a.listAndRestore(a.backupName, restoreFilesC)

	// wait for all workers to finish restoring the data files
	a.logger.Info("Waiting for all workers to finish")
	close(restoreFilesC)
	wg.Wait()

	a.logger.Debug("Creating missing required directories")
	if err := a.createRequiredDirs(); err != nil {
		a.logger.Error("Failed to create directory", zap.Error(err))
	}

	a.logger.Info(
		"Backup successfully restored",
		zap.Duration("seconds", time.Now().Sub(begin)),
	)

	return 0
}

func (a *app) createRequiredDirs() error {
	for _, d := range directoriesThatMustExist {
		// only try to create the directory if one does not already exist
		_, err := os.Stat(d)
		if os.IsNotExist(err) {
			if err := os.Mkdir(filepath.Join(*a.pgDataDirectory, d), 0700); err != nil {
				return err
			}
		}
	}

	return nil
}

// get the name of the last successful backup and update the configuration flag
func (a *app) resolveLatest() error {
	result, err := a.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: a.s3Bucket,
		Key:    aws.String(latestKey),
	})
	if err != nil {
		return err
	}

	defer result.Body.Close()

	// read the file
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, result.Body); err != nil {
		return err
	}

	// update the backup name
	*a.backupName = buf.String()

	return nil
}

// list all files with a given prefix and add them to the restoreFilexC channel
func (a *app) listAndRestore(prefix *string, restoreFilesC chan<- string) {
	a.logger.Debug("Listing folder", zap.String("prefix", *prefix))

	var next *string = nil
	for {
		input := &s3.ListObjectsV2Input{
			Bucket:    a.s3Bucket,
			Delimiter: aws.String("/"),
			Prefix:    prefix,
		}
		// include the continuation token, if there's one
		if next != nil {
			input.ContinuationToken = next
		}
		result, err := a.s3Client.ListObjectsV2(input)
		if err != nil {
			a.logger.Fatal("Failed to list S3 folder", zap.Error(err))
		}

		// objects to restore
		for _, obj := range result.Contents {
			if *obj.Key != *a.backupName+"/" {
				a.logger.Debug("Adding object", zap.String("key", *obj.Key))
				restoreFilesC <- *obj.Key
			}
		}

		// child folders to process
		for _, p := range result.CommonPrefixes {
			a.logger.Debug("Processing child folder", zap.String("prefix", *p.Prefix))
			a.listAndRestore(p.Prefix, restoreFilesC)
		}

		if *result.IsTruncated {
			next = result.NextContinuationToken
		} else {
			a.logger.Debug("Done with prefix", zap.String("prefix", *prefix))
			return
		}
	}
}

func (a *app) restoreWorker(restoreFilesC <-chan string, wg *sync.WaitGroup) {
	// continuously receive file paths (relative to the data directory)
	// from the filesC channel, add them to tar files of up to ~1GB, and upload them
	defer wg.Done()

	for {
		key, more := <-restoreFilesC
		if !more {
			a.logger.Debug("No more files to process")
			return
		}

		a.logger.Debug("Processing file", zap.String("remote", key))

		// drop the backup name from the key to get the path (relative to the data directory)
		dst := filepath.Join(*a.pgDataDirectory, strings.TrimPrefix(key, *a.backupName+"/"))

		// get the modify time stored in the object's metadata
		mtime := a.getMTime(key)
		// skip this file if the modify timestamp stored in the key's metadata matches the local version
		if *a.modifiedOnly && mtime != 0 {
			// the key on S3 may be of a compressed file in which case it'll include
			// an extension that the local file does not have
			local := strings.TrimSuffix(dst, lz4.Extension)
			if a.fileHasNotChanged(local, mtime) {
				a.logger.Debug("Skipping unmodified file", zap.String("remote", key))
				continue
			}
		}

		// if we've made it this far, the file needs to be restored
		a.logger.Debug("Restoring file", zap.String("remote", key), zap.String("local", dst))

		// make sure the directory path exists
		dir := filepath.Dir(dst)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			a.logger.Error("Failed to create the directory structure", zap.Error(err))
		}

		// create the local file
		out, err := os.Create(dst)
		if err != nil {
			a.logger.Error("Failed to create file", zap.Error(err))
		}
		// download contents
		_, err = a.s3Downloader.Download(
			out,
			&s3.GetObjectInput{
				Bucket: a.s3Bucket,
				Key:    aws.String(key),
			})
		if err != nil {
			a.logger.Error("Failed to download file", zap.Error(err))
		}

		// close the file
		if err := out.Close(); err != nil {
			a.logger.Error("Failed to close file", zap.Error(err))
		}

		// if what we got from S3 is a compressed file, decompress it and remove the compressed one
		localFile := out.Name()
		if util.IsCompressed(key) {
			compressed := out.Name()
			decompressed := strings.TrimSuffix(compressed, lz4.Extension)
			localFile = decompressed
			a.logger.Debug(
				"Decompressing file",
				zap.String("compressed", compressed),
				zap.String("decompressed", decompressed))
			if err := util.Decompress(compressed, decompressed); err != nil {
				a.logger.Error("Failed to decompress file", zap.Error(err))
			}
			a.mustRemoveFile(compressed)
		}

		// update the last modified time to match the one we just restored
		if mtime != 0 {
			a.logger.Debug("Updating mtime", zap.String("file", localFile), zap.Int64("time", mtime))
			if err := os.Chtimes(localFile, time.Now(), time.Unix(mtime, 0)); err != nil {
				a.logger.Error("Failed to update mtime", zap.Error(err))
			}
		}
	}
}

func (a *app) fileHasNotChanged(localFile string, mtime int64) bool {
	st, err := os.Stat(localFile)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		a.logger.Error("Failed to stat file", zap.Error(err))
		return false
	}

	return mtime == st.ModTime().Unix()
}

// return the modify timestamp stored in the objects metadata, or "" if it's not there
func (a *app) getMTime(key string) int64 {
	result, err := a.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: a.s3Bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		a.logger.Error("Failed to HEAD object", zap.Error(err))
		return 0
	}

	mtime, ok := result.Metadata[util.MetadataModifiedTime]
	if ok {
		mtime, err := strconv.Atoi(*mtime)
		if err != nil {
			a.logger.Error("Failed to HEAD object", zap.Error(err))
			return 0
		}

		return int64(mtime)
	}

	return 0
}

func parseRestoreBackupArgs(cfg *app, parser *argparse.Command) {
	cfg.modifiedOnly = parser.Flag(
		"",
		"modified-only",
		&argparse.Options{
			Required: false,
			Default:  false,
			Help:     "Use the last modified timestamp to transfer only files that have changed)"})
}
