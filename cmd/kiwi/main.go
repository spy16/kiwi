package main

import (
	"flag"
	"fmt"

	"github.com/spy16/kiwi"
)

var file = flag.String("file", "temp.kiwi", "Kiwi database file path")

func main() {
	db, err := kiwi.Open(*file, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println(db)
}
