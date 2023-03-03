package main

import (
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"
)

func run_script(t *testing.T, commands []string) []string {
	cmd := exec.Command("go", "run", "main.go")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}

	for _, cmd := range commands {
		io.WriteString(stdin, cmd+"\n")
	}

	out, err := io.ReadAll(stdout)
	if err != nil {
		t.Fatal(err)
	}

	err = cmd.Wait()
	if err != nil {
		t.Fatal(err)
	}

	return strings.Split(string(out), "\n")
}

func TestCmd(t *testing.T) {

	t.Run("tableFull", func(t *testing.T) {
		script := make([]string, 0)
		i := 1
		for i <= 1000 {
			script = append(script, fmt.Sprintf("insert %d user%d person%d@example.com",
				i, i, i))
			i++
		}
		script = append(script, ".exit")
		run_script(t, script)
	})

}
