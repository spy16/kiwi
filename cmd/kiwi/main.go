package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/spy16/kiwi"
)

var file = flag.String("file", "kiwi.db", "DB file path")
var index = flag.String("index", "b+tree", "Index to be used")

func main() {
	flag.Parse()

	db, err := kiwi.Open(*file, nil)
	if err != nil {
		log.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	fmt.Println(db)
}
