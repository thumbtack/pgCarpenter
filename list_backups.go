package main

import (
	"fmt"
	"sort"

	"github.com/akamensky/argparse"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

func parseListBackupsArgs(cfg *app, parser *argparse.Command) {
	// there are no options as of now, we just keep this around for consistency
	// (and easy maintenance/future-proof?)
}

func (a *app) listBackups() int {
	err := a.s3Client.ListObjectsV2Pages(
		&s3.ListObjectsV2Input{
			Bucket:    a.s3Bucket,
			Delimiter: aws.String("/"),
		},
		a.handleListBackupsPage)
	if err != nil {
		a.logger.Error("Failed to list S3 objects", zap.Error(err))
	}

	return 0
}

func (a *app) handleListBackupsPage(page *s3.ListObjectsV2Output, lastPage bool) bool {
	type backupEntry struct {
		name       string
		timestamp  string
		successful bool
	}

	format := "%-24s%-34s%s"
	backups := make([]backupEntry, 0)

	for _, prefix := range page.CommonPrefixes {
		// remove the trailing slash from the backup's name
		n := len(*prefix.Prefix)
		backupName := (*prefix.Prefix)[:n-1]
		// ignore the folder used to mark successful backups
		if backupName == successfullyCompletedFolder {
			continue
		}

		bkp := backupEntry{name: backupName, timestamp: ""}
		// try to get the object's last modified timestamp
		result, err := a.s3Client.GetObject(&s3.GetObjectInput{
			Bucket: a.s3Bucket,
			Key:    prefix.Prefix,
		})
		if err == nil {
			bkp.timestamp = result.LastModified.String()
		}

		// was this backup successfully completed?
		_, err = a.s3Client.GetObject(&s3.GetObjectInput{
			Bucket: a.s3Bucket,
			Key:    aws.String(successfullyCompletedFolder + "/" + backupName),
		})
		bkp.successful = err == nil

		backups = append(backups, bkp)
	}

	// sort by timestamp asc
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].timestamp < backups[j].timestamp
	})

	// formatted output
	fmt.Printf(format, "Name", "Created", "\n")

	for i, b := range backups {
		status := ""
		if !b.successful {
			status = "(incomplete!) "
		}
		fmt.Printf(format, b.name, b.timestamp, status)
		endLine := ""
		if i == len(backups)-1 {
			endLine = "(LATEST)"
		}
		fmt.Println(endLine)
	}

	return true
}
