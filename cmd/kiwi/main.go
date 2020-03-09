package main

import (
	"encoding/json"
	"flag"
	"os"

	"github.com/spy16/kiwi/blob"
	"github.com/spy16/kiwi/linearhash"
)

var file = flag.String("file", "kiwi.db", "DB file path")

func main() {
	flag.Parse()

	inmem := &blob.InMemStore{}

	lhs, err := linearhash.Open(*file, inmem, nil)
	if err != nil {
		panic(err)
	}
	defer lhs.Close()

	printStats(lhs)
}

func printStats(lhs *linearhash.Store) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(lhs.Stats())
}
