package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/akamensky/argparse"
	"github.com/pierrec/lz4"
	"github.com/thumbtack/pgCarpenter/util"
	"go.uber.org/zap"
)

func (a *app) archiveWAL() int {
	begin := time.Now()
	a.logger.Debug(
		"Starting to archive WAL segment",
		zap.String("WAL", *a.walPath))

	// full path to the WAL segment
	// (the path name PG passes along for the WAL segment is relative to the current working directory)
	walFullPath, err := a.getWALFullPath(*a.walPath)
	if err != nil {
		a.logger.Error("Failed to get the full path to the WAL segment", zap.Error(err))
		return 1
	}
	// object key (based on the file name, without the path, including the LZ4 extension)
	key := a.getWALObjectKey(walFullPath)
	// compress the WAL segment -- on a random sample of 256 WAL segments the file size was reduced to ~4.5MB, i.e.,
	// ~27% the original size (16MB)
	compressedWal, err := util.Compress(walFullPath, *a.tmpDirectory)
	if err != nil {
		a.logger.Error("Failed to compress WAL segment", zap.Error(err))
		return 1
	}
	// upload the compressed file
	err = a.storage.Put(key, compressedWal, 0)
	// regardless of whether or not the upload operation was successful, remove the compressed file
	util.MustRemoveFile(compressedWal, a.logger)
	// return non-zero on error
	if err != nil {
		a.logger.Error("Failed to upload WAL segment", zap.Error(err))
		return 1
	}

	a.logger.Debug(
		"Finished archiving WAL segment",
		zap.String("WAL", *a.walPath),
		zap.Duration("duration", time.Now().Sub(begin)))

	return 0
}

func (a *app) getWALFullPath(wal string) (string, error) {
	// the path name PG passes along for the WAL segment is relative to the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// full path to the WAL segment
	return filepath.Join(cwd, wal), nil
}

// create the object's key from the filename + LZ4 extension
func (a *app) getWALObjectKey(walPath string) string {
	return filepath.Join(walFolder, filepath.Base(walPath)+lz4.Extension)
}

func parseArchiveWALArgs(cfg *app, parser *argparse.Command) {
	// there are no options as of now, we just keep this around for consistency
	// (and easy maintenance/future-proof?)
}
