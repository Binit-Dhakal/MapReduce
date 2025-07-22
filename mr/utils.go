package mr

import (
	"hash/fnv"
	"strings"
)

type KeyValue struct {
	Key   string
	Value string
}

// Sort - map phase
type ByKey []KeyValue

func (k ByKey) Len() int           { return len(k) }
func (k ByKey) Less(i, j int) bool { return k[i].Key < k[j].Key }
func (k ByKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// method for RPC call to the Map worker
type MapWorkerLogic struct {
}

type ReduceSortItem struct {
	Key    string
	Value  string
	FileID int
}

type MinHeap []*ReduceSortItem

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Key < h[j].Key }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x any) {
	*h = append(*h, x.(*ReduceSortItem))
}

func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func parseLine(line string) (string, string) {
	parts := strings.SplitN(line, " ", 2)
	return parts[0], parts[1]
}
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
