package mr

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type ReduceWorker struct {
	workerAddr      string
	reduceID        int
	reducef         ReduceFunc
	taskID          int
	outputLocations []MapOutputLocation
	outputFiles     []string
}

func NewReduceWorker(workerAddr string, taskID int, reducef ReduceFunc, outputLocations []MapOutputLocation) *ReduceWorker {
	return &ReduceWorker{
		workerAddr:      workerAddr,
		reduceID:        taskID,
		reducef:         reducef,
		outputLocations: outputLocations,
	}
}

// 1. Find/request for all completed intermediate files and save to our local disk (shuffle)
// 2. Sort all intermediate files(external sort in disk)-not loading everything in memory
// 3. Group all intermediate keys together
// 4. call reduce function
func (r *ReduceWorker) ExecuteReduce() {
	r.findFileForPartition(r.outputLocations, r.reduceID)

	finalFile, err := os.OpenFile(
		fmt.Sprintf("reducer_%d", r.reduceID),
		os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		log.Fatalf("%v", err)
	}

	defer finalFile.Close()

	writer := bufio.NewWriter(finalFile)
	defer writer.Flush()

	var currentKey string
	var currentValues []string

	count := 0
	for sortedPair := range r.mergeSortIterator() {
		if currentKey == "" || sortedPair.Key != currentKey {
			if len(currentValues) > 0 {
				result := r.reducef(currentKey, currentValues)
				writer.WriteString(fmt.Sprintf("%s %v\n", currentKey, result))
			}
			// start new group
			currentKey = sortedPair.Key
			currentValues = []string{sortedPair.Value}
			count++
		} else {
			currentValues = append(currentValues, sortedPair.Value)
		}

		if count%1000 == 0 {
			writer.Flush()
		}
	}

	if len(currentValues) > 0 {
		result := r.reducef(currentKey, currentValues)
		writer.WriteString(fmt.Sprintf("%s %v\n", currentKey, result))
	}

	args := &ReduceTaskReportArgs{
		TaskID: r.reduceID,
		Status: Completed,
	}
	reply := &ReduceTaskReportReply{}
	ok := call("Coordinator.ReduceTaskReport", args, reply, coordinatorSock())
	if !ok {
		fmt.Println(ok)
		fmt.Println()
	}

	fmt.Println("Reduced finished")

}

// save all intermediate files(specific to this partition) to our local disk
func (r *ReduceWorker) findFileForPartition(outputs []MapOutputLocation, reduceID int) {
	tempDir := fmt.Sprintf("/tmp/mr_reducer_%d/", reduceID)
	os.MkdirAll(tempDir, 0755)

	for _, output := range outputs {
		filename := fmt.Sprintf("task-%d-part-%d", output.TaskID, output.PartID)
		args := GetIntermediateFileArgs{
			Filename: fmt.Sprintf("/tmp/%s", filename),
		}
		reply := GetIntermediateFileReply{}

		ok := call(
			"Worker.GetIntermediateFile",
			&args,
			&reply,
			output.WorkerAddress,
		)
		// TODO: inform coordinator that we couldn't connect with the worker so coordinator
		// can mark the worker as failed
		if !ok {
			log.Printf("Worker %s is not responding to get file %s", output.WorkerAddress, filename)
			continue
		}

		tempFilePath := filepath.Join(tempDir, filename)
		tempFile, err := os.Create(tempFilePath)
		if err != nil {
			log.Fatalf("Failed to create temp file: %v", err)
		}

		_, err = tempFile.Write(reply.Content)
		if err != nil {
			log.Fatalf("Failed to write to temp file: %v", err)
		}

		r.outputFiles = append(r.outputFiles, tempFilePath)
		tempFile.Close()
	}

}

func (r *ReduceWorker) mergeSortIterator() <-chan KeyValue {
	// TODO: try benchmarking with buffered channel to see if things speed up
	ch := make(chan KeyValue)

	go func() {
		defer close(ch)

		readers := make([]*bufio.Scanner, len(r.outputFiles))
		files := make([]*os.File, len(r.outputFiles))

		for i := range files {
			f, err := os.Open(r.outputFiles[i])
			if err != nil {
				panic(err)
			}
			files[i] = f
		}
		defer func() {
			for _, f := range files {
				f.Close()
			}
		}()

		for i, f := range files {
			readers[i] = bufio.NewScanner(f)
		}

		h := &MinHeap{}
		heap.Init(h)

		for i, reader := range readers {
			if reader.Scan() {
				key, val := parseLine(reader.Text())
				heap.Push(h, &ReduceSortItem{Key: key, Value: val, FileID: i})
			}
		}

		for h.Len() > 0 {
			item := heap.Pop(h).(*ReduceSortItem)
			ch <- KeyValue{Key: item.Key, Value: item.Value}

			reader := readers[item.FileID]
			if reader.Scan() {
				key, value := parseLine(reader.Text())
				heap.Push(h, &ReduceSortItem{Key: key, Value: value, FileID: item.FileID})
			}
		}

		if h.Len() == 0 {
			return
		}

	}()

	return ch
}
