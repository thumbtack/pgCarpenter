package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/akamensky/argparse"
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
	//   put full path to the remote storage object in the channel
	// workers:
	//   download the file to a.pgDataDirectory keeping the relative path
	//   e.g., s3://backupName/base/3456.gz --> a.pgDataDirectory/base/3456.gz
	//   decompress the file: a.pgDataDirectory/base/3456.gz --> a.pgDataDirectory/base/3456
	//
	// keep a counter of total number of files + number of files retrieved
	// print each time if in verbose mode

	// if requested, find the name of the latest backup and update the app struct
	if *a.backupName == latestKey {
		latest, err := a.resolveLatest()
		if err != nil {
			a.logger.Error("Failed to resolve the name of the backup for "+latestKey, zap.Error(err))
			return 1
		}
		// update the field with the backup name we'll be using everywhere
		*a.backupName = latest
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

	// kick off the (recursive) listing of all objects and put them in the restoreFilesC channel
	// so that the workers can restore the files
	if err := a.storage.WalkFolder(*a.backupName+"/", restoreFilesC); err != nil {
		a.logger.Error("Failed to traverse backup folder", zap.Error(err))
		return 1
	}

	// close the channel to signal there are no more items and wait for all workers to finish
	a.logger.Info("Waiting for all workers to finish")
	close(restoreFilesC)
	wg.Wait()

	a.logger.Debug("Creating missing required directories")
	a.createRequiredDirs()

	a.logger.Info(
		"Backup successfully restored",
		zap.Duration("seconds", time.Now().Sub(begin)),
	)

	return 0
}

func (a *app) createRequiredDirs() {
	for _, d := range directoriesThatMustExist {
		path := filepath.Join(*a.pgDataDirectory, d)
		// only try to create the directory if one does not already exist
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			if err := os.Mkdir(path, 0700); err != nil {
				// there's no benefit on interrupting the loop and returning an error
				// might as well just log it and move on to the next directory
				a.logger.Error("Failed to create directory", zap.Error(err))
			}
		}
	}
}

// get the name of the last successful backup and update the configuration flag
func (a *app) resolveLatest() (string, error) {
	latest, err := a.storage.GetString(latestKey)
	if err != nil {
		return "", err
	}

	return latest, nil
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

		// drop the backup name from the key to get the path relative to the data directory
		file := strings.TrimPrefix(key, *a.backupName+"/")
		dst := filepath.Join(*a.pgDataDirectory, file)

		// get the modify time stored in the object's metadata
		mtime, err := a.storage.GetLastModifiedTime(key)
		// skip this file if the modify timestamp stored in the key's metadata matches the local version
		if *a.modifiedOnly {
			if err != nil {
				a.logger.Error("Failed to get mtime", zap.Error(err), zap.String("key", key))
			} else {
				// the key may be of a compressed file in which case it'll include
				// an extension that the local file does not have
				local := strings.TrimSuffix(dst, lz4.Extension)
				if a.fileHasNotChanged(local, mtime) {
					a.logger.Debug("Skipping unmodified file", zap.String("remote", key))
					continue
				}
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
			// no point on trying to continue
			return
		}
		// download contents
		err = a.storage.Get(key, out)
		if err != nil {
			a.logger.Error("Failed to download file", zap.Error(err))
		}
		// close the file
		if err := out.Close(); err != nil {
			a.logger.Error("Failed to close file", zap.Error(err))
		}

		// if the object we got is a compressed file, decompress it and remove the compressed one
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
			util.MustRemoveFile(compressed, a.logger)
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

func parseRestoreBackupArgs(cfg *app, parser *argparse.Command) {
	cfg.modifiedOnly = parser.Flag(
		"",
		"modified-only",
		&argparse.Options{
			Required: false,
			Default:  false,
			Help:     "Use the last modified timestamp to transfer only files that have changed)"})
}
