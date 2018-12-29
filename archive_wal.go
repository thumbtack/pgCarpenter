package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/akamensky/argparse"
	"github.com/marcoalmeida/pgCarpenter/util"
	"github.com/pierrec/lz4"
	"go.uber.org/zap"
)

func (a *app) archiveWAL() int {
	begin := time.Now()
	a.logger.Debug(
		"Starting upload of WAL segment",
		zap.String("WAL", *a.walPath))

	// the path name PG passes along for the WAL segment is relative to the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		a.logger.Error("Failed to get current working directory", zap.Error(err))
		return 1
	}
	// full path to the WAL segment
	walFullPath := filepath.Join(cwd, *a.walPath)
	// create the object's key from the filename + LZ4 extension
	key := filepath.Join(walFolder, filepath.Base(walFullPath)+lz4.Extension)
	// compress the WAL segment -- on a random sample of 256 WAL segments the file size was reduced to ~4.5MB, i.e.,
	// ~27% the original size (16MB)
	compressedWal, err := util.Compress(walFullPath, *a.tmpDirectory)
	if err != nil {
		a.logger.Error("Failed to compress WAL segment", zap.Error(err))
		return 1
	}
	// upload the compressed file
	if err := a.storage.Put(key, compressedWal, 0); err != nil {
		a.logger.Error("Failed to upload WAL segment", zap.Error(err))
	}
	// remove the compressed file
	util.MustRemoveFile(compressedWal, a.logger)

	a.logger.Debug(
		"Finished uploading WAL segment",
		zap.String("WAL", *a.walPath),
		zap.Duration("duration", time.Now().Sub(begin)))

	return 0
}

func parseArchiveWALArgs(cfg *app, parser *argparse.Command) {
	// there are no options as of now, we just keep this around for consistency
	// (and easy maintenance/future-proof?)
}
