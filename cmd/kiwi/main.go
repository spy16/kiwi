package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/spy16/kiwi"
)

var file = flag.String("file", "kiwi.db", "DB file path")

func main() {
	flag.Parse()

	db, err := kiwi.Open(*file, nil)
	if err != nil {
		log.Fatalf("failed to open: %v", err)
	}

	printStats(db)
}

func printStats(db *kiwi.DB) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(db.Stats())
}
