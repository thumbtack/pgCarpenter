package main

import (
	"fmt"

	"github.com/akamensky/argparse"
)

func (a *app) restoreWAL() int {
	fmt.Println(*a.walPath)

	return 0
}

func parseRestoreWALArgs(cfg *app, parser *argparse.Command) {
	// there are no options as of now, we just keep this around for consistency
	// (and easy maintenance/future-proof?)
}
