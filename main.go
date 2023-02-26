package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"toydb-go/meta"
	"toydb-go/stmt"
)

// https://cstack.github.io/db_tutorial/parts/part2.html

func main() {
	table := stmt.NewTable()

	input := bufio.NewReader(os.Stdin)
	var sb strings.Builder
	for {
		print_prompt()
		sb.Reset()
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

		if strings.HasPrefix(cmd, ".") {
			err := meta.DoMetaCommand(cmd)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}

		statement, err := stmt.PrepareStatment(cmd)
		if err != nil {
			fmt.Println(err)
			continue
		}

		stmt.ExecuteStatement(statement, table)
	}
}

func print_prompt() {
	fmt.Printf("db > ")
}
