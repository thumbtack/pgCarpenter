package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/akamensky/argparse"
	_ "github.com/lib/pq"
	"github.com/marcoalmeida/pgCarpenter/util"
	"github.com/pierrec/lz4"
	"go.uber.org/zap"
)

// there's no point on taking backups of directories like pg_xlog
var prefixesNotToBackup = []string{"pg_xlog", "postmaster.pid", "pg_replslot"}

func (a *app) createBackup() int {
	a.logger.Info("Starting backup", zap.String("name", *a.backupName))
	begin := time.Now()

	backupKey := *a.backupName + "/"

	// don't allow existing backups to be overwritten
	_, err := a.storage.GetString(backupKey)
	if err == nil {
		a.logger.Error("A backup with the same name already exists", zap.String("backup_name", *a.backupName))
		return 1
	}

	// create the top level "folder" so that the object actually exists and
	// has all the relevant metadata like timestamps
	if err := a.storage.PutString(backupKey, ""); err != nil {
		a.logger.Error("Failed to create top-level backup folder", zap.Error(err))
		return 1
	}

	// tell PG we're starting a base backup, copy all the file, tell PG we're done
	db, err := a.startBackup()
	if err != nil {
		a.logger.Error("Failed to start backup", zap.Error(err))
		return 1
	}

	// copy all files to remote storage
	items := a.uploadFiles()

	// tell PG we're done copying the data directory, save the tablespace map and backup label files
	if err := a.stopBackup(db); err != nil {
		a.logger.Error("Failed to stop backup", zap.Error(err))
		return 1
	}

	// mark the backup as successful
	if err := a.putSuccessfulMarker(*a.backupName); err != nil {
		a.logger.Error("Failed to mark backup as successfully completed", zap.Error(err))
	}

	// update the LATEST marker
	if err := a.updateLatest(*a.backupName); err != nil {
		a.logger.Error("Failed to update the LATEST marker", zap.Error(err))
		return 1
	}

	a.logger.Info(
		"Backup successfully completed",
		zap.String("name", *a.backupName),
		zap.Int("files", items),
		zap.Duration("seconds", time.Now().Sub(begin)),
	)

	return 0
}

func (a *app) startBackup() (*sql.Conn, error) {
	d := time.Now().Add(time.Duration(*a.statementTimeout) * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	connStr := fmt.Sprintf("user=%s password='%s'", *a.pgUser, *a.pgPassword)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	_, err = conn.QueryContext(
		ctx,
		"SELECT pg_start_backup($1, $2, $3)",
		*a.backupName,
		*a.backupCheckpoint,
		*a.backupExclusive,
	)
	if err != nil {
		return nil, err
	}

	// if doing an exclusive backup we don't need to keep the connection open
	if *a.backupExclusive {
		err := db.Close()
		if err != nil {
			// it's an exclusive backup, we won't need the connection later on,
			// there's no point on returning the error
			a.logger.Error("Failed to close the DB connection", zap.Error(err))
		}
	}

	// when doing a non-exclusive backup connection calling pg_start_backup must be maintained until the end of the
	// backup, or the backup will be automatically aborted
	return conn, nil
}

func (a *app) stopBackup(conn *sql.Conn) error {
	d := time.Now().Add(time.Duration(*a.statementTimeout) * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// if doing an exclusive backup we'll need to create a new connection
	if *a.backupExclusive {
		connStr := fmt.Sprintf("user=%s password='%s'", *a.pgUser, *a.pgPassword)

		db, err := sql.Open("postgres", connStr)
		if err != nil {
			return err
		}

		_, err = db.Query("SELECT pg_stop_backup()")
		if err != nil {
			return err
		}
	} else {
		var lsn, labelFile, mapFile string

		row := conn.QueryRowContext(ctx, "SELECT * FROM pg_stop_backup(false)")
		err := row.Scan(&lsn, &labelFile, &mapFile)
		if err != nil {
			return err
		}

		// explicitly close the connection we kept open throughout the backup
		err = conn.Close()
		if err != nil {
			a.logger.Error("Failed to close connection", zap.Error(err))
		}

		// upload the second field to a file named backup_label in the root directory of the backup and
		// the third field to a file named tablespace_map, unless the field is empty
		key := *a.backupName + "/backup_label"
		err = a.storage.PutString(key, labelFile)
		if err != nil {
			return err
		}

		if mapFile != "" {
			key = *a.backupName + "/tablespace_map"
			err = a.storage.PutString(key, mapFile)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *app) getSuccessfulMarker(backupName string) string {
	return filepath.Join(successfullyCompletedFolder, backupName)
}

func (a *app) putSuccessfulMarker(backupName string) error {
	return a.storage.PutString(a.getSuccessfulMarker(backupName), "")
}

func (a *app) deleteSuccessfulMarker(backupName string) error {
	key := a.getSuccessfulMarker(backupName)
	_, err := a.storage.GetString(key)
	if err == nil {
		if err := a.storage.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

func (a *app) updateLatest(backupName string) error {
	return a.storage.PutString(latestKey, backupName)
}

// upload the data directory to remote storage; return the number of files uploaded
func (a *app) uploadFiles() int {
	// channel to keep the path of all files that need to compressed and uploaded
	filesC := make(chan string)

	// spawn a pool of workers
	a.logger.Info("Spawning workers", zap.Int("number", *a.nWorkers))
	wg := &sync.WaitGroup{}
	wg.Add(*a.nWorkers)
	for i := 0; i < *a.nWorkers; i++ {
		go a.backupWorker(filesC, wg)
	}

	// traverse the data directory and put each file (relative path) in the channel for a worker to process
	a.logger.Debug("Walking data directory", zap.String("path", *a.pgDataDirectory))
	items := 0
	err := filepath.Walk(
		*a.pgDataDirectory,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				// files might change during the copy process; it's normal during an online backup
				if os.IsNotExist(err) {
					a.logger.Debug("Source file vanished", zap.String("path", path), zap.Error(err))
					return nil
				}
				// anything other than the file not existing, on the other hand, is a problem
				return err
			}
			// grab just the path relative to the data directory
			file := strings.TrimPrefix(path, *a.pgDataDirectory)
			if a.ignoreFile(file) {
				a.logger.Debug("Ignoring file", zap.String("path", path))
				return nil
			}
			a.logger.Debug("Adding file", zap.String("path", file))
			filesC <- file
			items++
			return nil
		},
	)

	if err != nil {
		a.logger.Error("Failed to walk data directory", zap.Error(err))
		return 1
	}

	a.logger.Info("Waiting for all workers to finish")
	close(filesC)
	wg.Wait()

	return items
}

// return true iff it's in one of the directories we do not need to backup
func (a *app) ignoreFile(path string) bool {
	for _, d := range prefixesNotToBackup {
		if strings.HasPrefix(path, d) {
			return true
		}
	}

	return false
}

// continuously receive file paths (relative to the data directory) from the filesC channel
// compress the ones larger than compress-threshold, and upload them to remote storage along with some relevant metadata
func (a *app) backupWorker(filesC <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		pgFile, more := <-filesC
		if !more {
			a.logger.Debug("No more files to process")
			return
		}

		pgFilePath := filepath.Join(*a.pgDataDirectory, pgFile)
		st, err := os.Stat(pgFilePath)
		if err != nil {
			// this can happen for very legitimate reasons, as PG is not stopped and we're taking an online backup
			a.logger.Info("Failed to stat file. Might have been removed", zap.Error(err))
			continue
		}

		// skip directories
		if st.IsDir() {
			a.logger.Debug("Ignoring directory", zap.String("path", pgFile))
			continue
		}

		// name the object after the file path relative to the data directory
		key := filepath.Join(*a.backupName, pgFile)
		// compress files larger than a given threshold
		compressed := ""
		if st.Size() > int64(*a.compressThreshold) {
			a.logger.Debug("Compressing file", zap.String("path", pgFile), zap.Int64("size", st.Size()))
			compressed, err = util.Compress(pgFilePath, *a.tmpDirectory)
			if err != nil {
				a.logger.Error("Failed to compress file", zap.Error(err))
				// we use compressed == "" to decide whether to upload and remove a compressed file
				// let's try to proceed with the backup by uploading the uncompressed file
				compressed = ""
				continue
			}
			// mark the object as a compressed file
			key += lz4.Extension

		}

		if compressed != "" {
			err = a.storage.Put(key, compressed, st.ModTime().Unix())
			// cleanup the temporary compressed file
			util.MustRemoveFile(compressed, a.logger)
		} else {
			err = a.storage.Put(key, pgFilePath, st.ModTime().Unix())
		}

		if err != nil {
			a.logger.Fatal("Failed to upload file", zap.Error(err))
		}
	}
}

func parseCreateBackupArgs(cfg *app, parser *argparse.Command) {
	cfg.compressThreshold = parser.Int(
		"",
		"compress-threshold",
		&argparse.Options{
			Required: false,
			Default:  512 * 1024,
			Help:     "compress files larger than"})
	cfg.pgUser = parser.String(
		"",
		"user",
		&argparse.Options{
			Required: false,
			Default:  "postgres",
			Help:     "PostgreSQL user"})
	cfg.pgPassword = parser.String(
		"",
		"password",
		&argparse.Options{
			Required: false,
			Default:  "",
			Help:     "PostgreSQL password"})
	cfg.backupCheckpoint = parser.Flag(
		"",
		"checkpoint",
		&argparse.Options{
			Required: false,
			Default:  false,
			Help:     "Start the backup as soon as possible by issuing an checkpoint"})
	cfg.backupExclusive = parser.Flag(
		"",
		"exclusive",
		&argparse.Options{
			Required: false,
			Default:  false,
			Help:     "Disallow other concurrent backups (the backup can only be taken on a primary)"})
	cfg.statementTimeout = parser.Int(
		"",
		"statement-timeout",
		&argparse.Options{
			Required: false,
			Default:  60,
			Help:     "Cancel a start/stop backup statement if it takes more than the specified number of seconds"})
}
