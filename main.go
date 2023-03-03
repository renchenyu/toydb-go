package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"toydb-go/stmt"
)

// https://cstack.github.io/db_tutorial/parts/part2.html

func main() {
	filename := "test.db"
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		if err := stmt.CreateTable(filename); err != nil {
			panic(err)
		}
	}

	table, err := stmt.OpenTable(filename)
	if err != nil {
		panic(err)
	}

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
			if cmd == ".exit" {
				if err := table.Close(); err != nil {
					panic(err)
				}
				os.Exit(0)
			} else if cmd == ".len" {
				fmt.Printf("%d\n", table.Len())
			} else if cmd == ".dump" {
				table.Dump()
			} else if cmd == ".info" {
				table.Info()
			} else {
				fmt.Printf("unrecognized meta command: %q\n", cmd)
			}
			continue
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
