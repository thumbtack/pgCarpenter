package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/akamensky/argparse"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	successfullyCompletedFolder = "successful"
	latestKey                   = "LATEST"
	backupNameRE                = "^[a-zA-Z0-9_-]+$"
)

type app struct {
	s3Region          *string
	s3Bucket          *string
	s3MaxRetries      *int
	backupName        *string
	nWorkers          *int
	pgUser            *string
	pgPassword        *string
	pgDataDirectory   *string
	backupCheckpoint  *bool
	backupExclusive   *bool
	statementTimeout  *int
	compressThreshold *int
	modifiedOnly      *bool
	tmpDirectory      *string
	verbose           *bool
	s3Client          *s3.S3
	s3Uploader        *s3manager.Uploader
	s3Downloader      *s3manager.Downloader
	logger            *zap.Logger
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
			Required: len(os.Args) > 1 && (os.Args[1] == "create-backup" || os.Args[1] == "restore-backup"),
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
			Help:     "Directory to use for temporary files"})
	a.verbose = parser.Flag(
		"",
		"verbose",
		&argparse.Options{
			Required: false,
			Default:  false,
			Help:     "Verbose output"})

	// subcommands
	listBackupsCmd := parser.NewCommand("list-backups", "List all available backups")
	parseListBackupsArgs(a, listBackupsCmd)
	createBackupCmd := parser.NewCommand("create-backup", "Create a new base backup storing it on S3")
	parseCreateBackupArgs(a, createBackupCmd)
	restoreBackupCmd := parser.NewCommand("restore-backup", "Restore a base backup from S3")
	parseRestoreBackupArgs(a, restoreBackupCmd)
	// TODO: archive-wal, fetch-wal, delete-backup

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

	// for some reason, on some servers, filepath.Walk will not traverse
	// the directory unless the path ends with a trailing slash
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

	// create the S3 client before calling the callback
	cfg.s3Client = s3.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(*cfg.s3Region).
				WithMaxRetries(*cfg.s3MaxRetries),
		)))
	// the s3 manager is helpful with large file uploads; also thread-safe
	cfg.s3Uploader = s3manager.NewUploaderWithClient(cfg.s3Client, func(u *s3manager.Uploader) {
		u.PartSize = 32 * 1024 * 1024
		u.Concurrency = 32
		u.LeavePartsOnError = false
	})
	cfg.s3Downloader = s3manager.NewDownloaderWithClient(cfg.s3Client, func(u *s3manager.Downloader) {
		u.PartSize = 32 * 1024 * 1024
		u.Concurrency = 32
	})

	// make sure we're using the absolute path to the data directory before starting
	if err := cfg.normalizeDataDirectoryPath(); err != nil {
		cfg.logger.Error("Failed to normalize the path to the data directory", zap.Error(err))
		os.Exit(1)
	}

	os.Exit(callback())
}
