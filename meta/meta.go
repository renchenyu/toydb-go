package meta

import (
	"fmt"
	"os"
)

func DoMetaCommand(cmd string) error {
	if cmd == ".exit" {
		os.Exit(0)
		return nil
	} else {
		return fmt.Errorf("unrecognized meta command: %q", cmd)
	}
}
