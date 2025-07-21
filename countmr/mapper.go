package main

import (
	"strings"
	"unicode"

	"github.com/Binit-Dhakal/mapreduce/mr"
)

func Map(filename string, content string) []mr.KeyValue {
	ff := func(r rune) bool {
		return !unicode.IsLetter(r)
	}

	words := strings.FieldsFunc(content, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}

	return kva
}
