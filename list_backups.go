package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/akamensky/argparse"
	"go.uber.org/zap"
)

func (a *app) listBackups() int {
	type backupEntry struct {
		name       string
		timestamp  int64
		successful bool
	}

	format := "%-34s%-28s%s"
	backups := make([]backupEntry, 0)

	// fetch all keys at the root of the bucket
	keys, err := a.storage.ListFolder("")
	if err != nil {
		a.logger.Error("Failed to list backups", zap.Error(err))
	}

	for _, k := range keys {
		// remove the trailing slash from the backup's name
		backupName := k[:len(k)-1]
		// ignore the folder used to mark successful backups and the one we keep WAL segments in
		if backupName == successfullyCompletedFolder || backupName == walFolder {
			continue
		}

		bkp := backupEntry{name: backupName, timestamp: 0}
		// try to get the object's last modified timestamp
		mtime, err := a.storage.GetLastModifiedTime(k)
		if err == nil {
			bkp.timestamp = mtime
		}

		// was this backup successfully completed?
		_, err = a.storage.GetString(a.getSuccessfulMarker(backupName))
		bkp.successful = err == nil

		backups = append(backups, bkp)
	}

	// try to get the name of the latest backup
	latest, err := a.storage.GetString(latestKey)
	if err != nil {
		latest = ""
	}

	// sort by timestamp asc
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].timestamp < backups[j].timestamp
	})

	// formatted output
	fmt.Printf(format, "Name", "Created", "\n")
	for _, b := range backups {
		fmt.Printf(format, b.name, formatTime(b.timestamp), formatStatus(b.successful))
		endLine := ""
		if b.name == latest {
			endLine = "(LATEST)"
		}
		fmt.Println(endLine)
	}

	return 0
}

func formatTime(mtime int64) string {
	t := time.Unix(mtime, 0)

	return t.Format(time.RFC3339)
}

func formatStatus(success bool) string {
	if !success {
		return "(incomplete!) "
	}

	return ""
}

func parseListBackupsArgs(cfg *app, parser *argparse.Command) {
	// there are no options as of now, we just keep this around for consistency
	// (and easy maintenance/future-proof?)
}
