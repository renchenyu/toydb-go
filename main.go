package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	input := bufio.NewReader(os.Stdin)
	var sb strings.Builder
	for {
		print_prompt()
		for {
			b, isPrefix, err := input.ReadLine()
			if err != nil {
				log.Fatal(err)
			}
			_, err = sb.Write(b)
			if err != nil {
				log.Fatal(err)
			}
			if !isPrefix {
				break
			}
		}

		cmd := sb.String()

		if cmd == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command %q.\n", cmd)
		}
		sb.Reset()
	}
}

func print_prompt() {
	fmt.Printf("db > ")
}
