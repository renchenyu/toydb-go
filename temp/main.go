package main

import "fmt"

func main() {
	for i := 1; i < 100000; i++ {
		fmt.Printf("insert %d a a\n", i)
	}
	fmt.Println(".exit")
}
