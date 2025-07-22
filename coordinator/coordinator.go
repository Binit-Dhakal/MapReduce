package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Binit-Dhakal/mapreduce/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: coordinator.go ...inputfiles")
	}

	m := mr.NewCoordinator(os.Args[1:], 1)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

}
