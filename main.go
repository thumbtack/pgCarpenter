package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/akamensky/argparse"
	"github.com/marcoalmeida/pgCarpenter/storage"
	"github.com/marcoalmeida/pgCarpenter/storage/s3storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	walFolder                   = "WAL"
	successfullyCompletedFolder = "successful"
	latestKey                   = "LATEST"
	backupNameRE                = "^[a-zA-Z0-9_-]+$"
)

type app struct {
	// common
	s3Region        *string
	s3Bucket        *string
	s3MaxRetries    *int
	backupName      *string // only required by create, restore, and delete
	pgDataDirectory *string // only required by create and restore
	nWorkers        *int    // only create and restore can effectively use > 1
	walPath         *string // only required by archive-wal and restore-wal
	tmpDirectory    *string
	verbose         *bool
	// set on create_backup.go
	pgUser            *string
	pgPassword        *string
	backupCheckpoint  *bool
	backupExclusive   *bool
	statementTimeout  *int
	compressThreshold *int
	// set on restore_backup.go
	modifiedOnly *bool
	// internal
	storage storage.Storage
	logger  *zap.Logger
}

func initLogging() (*zap.Logger, *zap.AtomicLevel) {
	atom := zap.NewAtomicLevel()
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	return zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			atom),
		),
		&atom
}

// parse command line arguments, populate the app struct,
// and return the callback function that should be executed
func parseArgs(a *app) func() int {
	parser := argparse.NewParser(
		"pgCarpenter",
		"PostgreSQL Continuous Archiving and Point-in-Time Recovery")

	// flags common to all sub-commands
	a.s3Region = parser.String(
		"",
		"s3-region",
		&argparse.Options{
			Required: false,
			Default:  "us-east-1",
			Help:     "AWS region where the S3 bucket lives in"})
	a.s3Bucket = parser.String(
		"",
		"s3-bucket",
		&argparse.Options{
			Required: true,
			Help:     "S3 bucket where to push/fetch backups to/from"})
	a.s3MaxRetries = parser.Int(
		"",
		"s3-max-retries",
		&argparse.Options{
			Required: false,
			Default:  3,
			Help:     "Maximum number of attempts at connecting to S3"})
	a.backupName = parser.String(
		"",
		"backup-name",
		&argparse.Options{
			Required: len(os.Args) > 1 &&
				(os.Args[1] == "create-backup" || os.Args[1] == "restore-backup" || os.Args[1] == "delete-backup"),
			Validate: validateBackupName,
			Help:     "Name of the backup"})
	a.pgDataDirectory = parser.String(
		"",
		"data-directory",
		&argparse.Options{
			Required: len(os.Args) > 1 && (os.Args[1] == "create-backup" || os.Args[1] == "restore-backup"),
			Validate: validateDataDirectory,
			Help:     "Full path to the data directory of the PostgreSQL cluster to backup"})
	a.nWorkers = parser.Int(
		"",
		"workers",
		&argparse.Options{
			Required: false,
			Default:  1,
			Help:     "Number of concurrent jobs"})
	a.tmpDirectory = parser.String(
		"",
		"tmp",
		&argparse.Options{
			Required: false,
			Default:  "/tmp",
			Help:     "Directory to use for creating temporary files"})
	a.verbose = parser.Flag(
		"",
		"verbose",
		&argparse.Options{
			Required: false,
			Default:  false,
			Help:     "Verbose output"})
	// archive WAL + rstore WAL
	a.walPath = parser.String(
		"",
		"wal-path",
		&argparse.Options{
			Required: len(os.Args) > 1 && (os.Args[1] == "archive-wal" || os.Args[1] == "restore-wal"),
			Help:     "Path to the WAL file"})

	// subcommands
	listBackupsCmd := parser.NewCommand("list-backups", "List all available backups")
	parseListBackupsArgs(a, listBackupsCmd)
	createBackupCmd := parser.NewCommand("create-backup", "Create a new base backup storing it on S3")
	parseCreateBackupArgs(a, createBackupCmd)
	restoreBackupCmd := parser.NewCommand("restore-backup", "Restore a base backup from S3")
	parseRestoreBackupArgs(a, restoreBackupCmd)
	archiveWALCmd := parser.NewCommand("archive-wal", "Archive a WAL segment (use with archive_command)")
	parseArchiveWALArgs(a, archiveWALCmd)
	restoreWALCmd := parser.NewCommand("restore-wal", "Restore a WAL segment (use with restore_command)")
	parseRestoreWALArgs(a, restoreWALCmd)
	// TODO: delete-backup

	// parse input
	err := parser.Parse(os.Args)
	if err != nil {
		// print the error message and usage information
		// (just like with the -h or --help flags)
		fmt.Print(parser.Usage(err))
		// essentially a no-op
		return func() int { return 1 }
	}

	if listBackupsCmd.Happened() {
		return a.listBackups
	}
	if createBackupCmd.Happened() {
		return a.createBackup
	}
	if restoreBackupCmd.Happened() {
		return a.restoreBackup
	}
	if archiveWALCmd.Happened() {
		return a.archiveWAL
	}
	if restoreWALCmd.Happened() {
		return a.restoreWAL
	}

	// we should never reach this point, but the compiler needs it
	return func() int { return 1 }
}

func validateDataDirectory(args []string) error {
	// make sure the data directory exists before starting
	st, err := os.Stat(args[0])
	if os.IsNotExist(err) {
		return errors.New("data directory not found: " + args[0])
	}

	if !st.IsDir() {
		return errors.New("path to data directory is not a directory: " + args[0])
	}

	return nil
}

func validateBackupName(args []string) error {
	// make sure the backup name is valid
	errorMsg := fmt.Sprintf("backup name ('%s') does not match '%s'", args[0], backupNameRE)
	if args[0] != latestKey {
		match, err := regexp.MatchString(backupNameRE, args[0])
		if err != nil || !match {
			return errors.New(errorMsg)
		}
	}

	return nil
}

// make sure we have the absolute path to the data directory
func (a *app) normalizeDataDirectoryPath() error {
	// get the absolute path
	dataDirectory, err := filepath.Abs(*a.pgDataDirectory)
	if err != nil {
		return err
	}

	// the data directory may be a symlink, in which case filepath.Walk will not traverse
	// it unless the path ends with a trailing slash
	if dataDirectory[len(dataDirectory)-1:] != "/" {
		dataDirectory += "/"
	}

	// update the value of the app struct, used everywhere
	*a.pgDataDirectory = dataDirectory
	a.logger.Debug("Updated data directory", zap.String("path", *a.pgDataDirectory))

	return nil
}

func main() {
	// logging
	logger, atom := initLogging()
	// flush the buffer before exiting
	defer logger.Sync()

	cfg := &app{
		logger: logger,
	}

	// parse the command line arguments and get a callback to the subcommand we should execute
	callback := parseArgs(cfg)

	// adjust the log level
	if *cfg.verbose {
		atom.SetLevel(zap.DebugLevel)
	}

	// as of now the only supported storage backend is S3
	cfg.storage = s3storage.New(*cfg.s3Bucket, *cfg.s3Region, *cfg.s3MaxRetries, cfg.logger)

	// make sure we're using the absolute path to the data directory before starting
	if err := cfg.normalizeDataDirectoryPath(); err != nil {
		cfg.logger.Error("Failed to normalize the path to the data directory", zap.Error(err))
		os.Exit(1)
	}

	os.Exit(callback())
}
