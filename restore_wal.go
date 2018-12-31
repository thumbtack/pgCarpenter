package main

import (
	"io/ioutil"
	"regexp"
	"time"

	"github.com/akamensky/argparse"
	"github.com/marcoalmeida/pgCarpenter/util"
	"go.uber.org/zap"
)

func (a *app) restoreWAL() int {
	begin := time.Now()
	a.logger.Debug(
		"Starting to restore WAL segment",
		zap.String("WAL filename", *a.walFileName),
		zap.String("WAL path", *a.walPath))

	// full path to the WAL segment
	// (the path name PG passes along for the WAL segment is relative to the current working directory)
	walFullPath, err := a.getWALFullPath(*a.walPath)
	if err != nil {
		a.logger.Error("Failed to get the full path to the WAL segment", zap.Error(err))
		return 1
	}

	// ignore history files (matching [0-9].history):
	// https://www.postgresql.org/docs/8.3/continuous-archiving.html (24.3.3)
	match, err := regexp.MatchString(`[0-9]+\.history`, *a.walFileName)
	if err == nil && match {
		a.logger.Debug("Ignoring history file", zap.String("filename", *a.walFileName))
		return 0
	}

	// object key (based on the file name, without the path, including the LZ4 extension)
	key := a.getWALObjectKey(*a.walFileName)
	// download to a temporary file
	outTmp, err := ioutil.TempFile(*a.tmpDirectory, "")
	// don't exit without trying to remove the temporary file
	defer util.MustRemoveFile(outTmp.Name(), a.logger)
	// get the contents of the (compressed) WAL segment to the temporary file
	err = a.storage.Get(key, outTmp)
	if err != nil {
		// this may not be an error. it's possible for
		a.logger.Info(
			"Failed to download WAL segment. This may not be an error (e.g., WAL has not yet been archived)",
			zap.Error(err),
			zap.String("key", key),
			zap.String("filename", *a.walFileName))
		return 1
	}
	// close the file
	if err := outTmp.Close(); err != nil {
		a.logger.Error("Failed to close temporary WAL segment", zap.Error(err))
		// it's not safe to report that the file is available and in a good state
		return 1
	}
	// decompress the temporary file to the requested WAL segment
	if err := util.Decompress(outTmp.Name(), walFullPath); err != nil {
		a.logger.Error("Failed to decompress temporary WAL segment", zap.Error(err))
		return 1
	}

	a.logger.Debug(
		"Finished restoring WAL segment",
		zap.String("WAL", *a.walPath),
		zap.Duration("duration", time.Now().Sub(begin)))

	return 0
}

func parseRestoreWALArgs(cfg *app, parser *argparse.Command) {
	cfg.walFileName = parser.String(
		"",
		"wal-filename",
		&argparse.Options{
			// Required: len(os.Args) > 1 && (os.Args[1] == "archive-wal" || os.Args[1] == "restore-wal"),
			Required: true,
			Help:     "File name of the desired WAL segment"})
}
