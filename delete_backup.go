package main

import (
	"github.com/akamensky/argparse"
	"go.uber.org/zap"
	"sync"
	"time"
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
		a.logger.Error("Failed to traverse backup folder", zap.Error(err))
		return 1
	}

	// close the channel to signal there are no more items and wait for all workers to finish
	a.logger.Info("Waiting for all workers to finish")
	close(keysC)
	wg.Wait()

	// remove the top level folder
	if err := a.storage.Delete(*a.backupName + "/"); err != nil {
		a.logger.Error("Failed to delete the top level folder", zap.Error(err))
		return 1
	}

	a.logger.Info(
		"Backup successfully deleted",
		zap.Duration("seconds", time.Now().Sub(begin)),
	)

	return 0
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

func parseDeleteBackupArgs(cfg *app, parser *argparse.Command) {
	// there are no options as of now, we just keep this around for consistency
	// (and easy maintenance/future-proof?)
}
