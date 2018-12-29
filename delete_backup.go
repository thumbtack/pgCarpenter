package main

import (
	"strings"
	"sync"
	"time"

	"github.com/akamensky/argparse"
	"go.uber.org/zap"
)

func (a *app) DeleteBackup() int {
	a.logger.Info("Starting to delete backup", zap.String("name", *a.backupName))
	begin := time.Now()

	// make sure the backup exists
	_, err := a.storage.GetString(*a.backupName + "/")
	if err != nil {
		a.logger.Error("Backup not found", zap.String("name", *a.backupName), zap.Error(err))
		return 1
	}

	// traverse the backup directory and delete all objects
	if err := a.traverseAndDelete(); err != nil {
		a.logger.Error("Failed to traverse backup folder", zap.Error(err))
		return 1
	}

	// remove the top level folder
	if err := a.storage.Delete(*a.backupName + "/"); err != nil {
		a.logger.Error("Failed to delete the top level folder", zap.Error(err))
		return 1
	}

	// remove the successful marker, if one exists
	if err := a.deleteSuccessfulMarker(*a.backupName); err != nil {
		a.logger.Error("Failed to delete successful marker", zap.Error(err))
	}

	// update the reference to LATEST
	a.updateReferenceToLatest()

	a.logger.Info(
		"Backup successfully deleted",
		zap.Duration("seconds", time.Now().Sub(begin)),
	)

	return 0
}

func (a *app) traverseAndDelete() error {
	// channel to keep the path of all files that need to compressed and uploaded
	keysC := make(chan string)

	// spawn a pool of workers
	a.logger.Info("Spawning workers", zap.Int("number", *a.nWorkers))
	wg := &sync.WaitGroup{}
	wg.Add(*a.nWorkers)
	for i := 0; i < *a.nWorkers; i++ {
		go a.deleteWorker(keysC, wg)
	}

	// kick off the (recursive) listing of all objects and storing their path in the keysC channel
	if err := a.storage.WalkFolder(*a.backupName+"/", keysC); err != nil {
		return err
	}

	// close the channel to signal there are no more items and wait for all workers to finish
	a.logger.Info("Waiting for all workers to finish")
	close(keysC)
	wg.Wait()

	return nil
}

func (a *app) deleteWorker(keysC <-chan string, wg *sync.WaitGroup) {
	// continuously receive file paths (relative to the data directory)
	// from the filesC channel, add them to tar files of up to ~1GB, and upload them
	defer wg.Done()

	for {
		key, more := <-keysC
		if !more {
			a.logger.Debug("No more files to delete")
			return
		}

		a.logger.Debug("Deleting file", zap.String("key", key))
		if err := a.storage.Delete(key); err != nil {
			a.logger.Error("Failed to delete file", zap.String("key", key))
		}
	}
}

func (a *app) updateReferenceToLatest() {
	latest, err := a.resolveLatest()
	if err != nil {
		// nothing we can do
		a.logger.Error("Failed to resolve the reference to LATEST", zap.Error(err))
		return
	}
	a.logger.Debug("Found LATEST", zap.String("key", latest))

	// if the backup we just deleted is not LATEST, there's nothing for us to do here
	if *a.backupName != latest {
		return
	}

	// fetch all allBackups at the root of the bucket
	allBackups, err := a.storage.ListFolder("")
	if err != nil {
		a.logger.Error("Failed to get all backups", zap.Error(err))
	}

	// point LATEST to the most recent successful backup
	newLatestKey := ""
	newLatestMTime := int64(0)
	for _, bkp := range allBackups {
		mtime, err := a.storage.GetLastModifiedTime(bkp)
		if err == nil {
			_, err = a.storage.GetString(a.getSuccessfulMarker(bkp))
			if err == nil {
				if mtime > newLatestMTime {
					a.logger.Debug(
						"Found most recent backup",
						zap.String("key", bkp),
						zap.Int64("mtime", mtime))
					newLatestKey = bkp
					newLatestMTime = mtime
				}
			}
		}
	}

	if newLatestMTime != 0 {
		// remove the trailing slash from the key to get the backup name
		newLatestName := strings.TrimSuffix(newLatestKey, "/")
		if err := a.updateLatest(newLatestName); err != nil {
			a.logger.Error("Failed to update the reference to LATEST", zap.Error(err))
		}
	}
}

func parseDeleteBackupArgs(cfg *app, parser *argparse.Command) {
	// there are no options as of now, we just keep this around for consistency
	// (and easy maintenance/future-proof?)
}
