package mr

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type ReduceWorker struct {
	reduceID    int
	reducef     ReduceFunc
	taskID      int
	outputFiles []string
}

func NewReduceWorker(taskID int, reduceID int, reducef ReduceFunc) *ReduceWorker {
	return &ReduceWorker{
		taskID:      taskID,
		reduceID:    reduceID,
		reducef:     reducef,
		outputFiles: make([]string, 0),
	}
}

// 1. Find/request for all completed intermediate files and save to our local disk (shuffle)
// 2. Sort all intermediate files(external sort in disk)-not loading everything in memory
// 3. Group all intermediate keys together
// 4. call reduce function
func (r *ReduceWorker) ExecuteReduce() error {
	r.findFileForPartition(r.reduceID)

	finalFile, err := os.OpenFile(
		fmt.Sprintf("reducer_%d", r.reduceID),
		os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
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

	return nil
}

// save all intermediate files(specific to this partition) to our local disk
func (r *ReduceWorker) findFileForPartition(reduceID int) {
	tempDir := fmt.Sprintf("/tmp/mr_reducer_%d/", reduceID)
	os.MkdirAll(tempDir, 0755)

	visitedFiles := make(map[string]struct{})

	totalCount := 0
	curCount := 0
	for {
		wasFailure := false
		fmt.Println("Pinging for new data")

		getLocationsArgs := GetReduceInputLocationArgs{PartitionID: reduceID}
		getLocationsReply := GetReduceInputLocationReply{}

		ok := call("Coordinator.GetReduceInputLocation", &getLocationsArgs, &getLocationsReply, coordinatorSock())
		if !ok {
			log.Printf("Failed to get updated map output location from coordinator.")
			time.Sleep(time.Second)
			continue
		}

		totalCount = getLocationsReply.TotalFiles

		for address, filenames := range getLocationsReply.Locations {
			for _, filename := range filenames {
				if _, ok := visitedFiles[filename]; ok {
					continue
				}

				args := GetIntermediateFileArgs{
					Filename: filename,
				}
				reply := GetIntermediateFileReply{}

				ok := call(
					"Worker.GetIntermediateFile",
					&args,
					&reply,
					address,
				)
				// TODO: inform coordinator that we couldn't connect with the worker so coordinator
				// can mark the worker as failed
				if !ok {
					wasFailure = true
					log.Printf("Worker %s is not responding to get file %s", address, filename)

					reportArgs := ReportFileInaccessibleArgs{WorkerAddress: address}
					reportReply := ReportFileInaccessibleReply{}

					call("Coordinator.ReportFileInaccessible", reportArgs, reportReply, coordinatorSock())
					break
				}

				visitedFiles[filename] = struct{}{}
				curCount++

				filename = filepath.Base(filename)
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
		if wasFailure || (curCount < totalCount) {
			time.Sleep(time.Second)
		} else {
			break
		}
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
