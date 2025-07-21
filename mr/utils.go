package mr

type KeyValue struct {
	Key   string
	Value string
}

// Sort
type ByKey []KeyValue

func (k ByKey) Len() int           { return len(k) }
func (k ByKey) Less(i, j int) bool { return k[i].Key < k[j].Key }
func (k ByKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) []string

// method for RPC call to the Map worker
type MapWorkerLogic struct {
}
