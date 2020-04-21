package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/spy16/kiwi"
)

var file = flag.String("file", "kiwi.db", "DB file path")

func main() {
	flag.Parse()

	db, err := kiwi.Open(*file, nil, nil)
	if err != nil {
		log.Fatalf("failed to open: %v", err)
	}

	fmt.Println(db)
}
