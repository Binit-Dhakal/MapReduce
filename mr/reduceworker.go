package mr

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type ReduceWorker struct {
	workerID        int
	reduceID        int
	reducef         ReduceFunc
	outputLocations []MapOutputLocation
}

func NewReduceWorker(workerID int, reduceID int, reducef ReduceFunc, outputLocations []MapOutputLocation) *ReduceWorker {
	return &ReduceWorker{
		workerID:        workerID,
		reduceID:        reduceID,
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
}

// save all intermediate files(specific to this partition) to our local disk
func (r *ReduceWorker) findFileForPartition(outputs []MapOutputLocation, reduceID int) {
	tempDir := fmt.Sprintf("/tmp/mr_reducer_%d/", reduceID)
	os.MkdirAll(tempDir, 0755)

	for _, output := range outputs {
		filename := fmt.Sprintf("task-%d-part-%d", output.TaskID, output.PartID)
		args := GetIntermediateFileArgs{
			Filename: filename,
		}
		reply := GetIntermediateFileReply{}

		ok := call(
			"Worker.GetIntermediateFile",
			&args,
			&reply,
			output.WorkerAddress,
		)
		if !ok {
			log.Printf("Worker %s is not responding to get file %s", output.WorkerAddress, filename)
			// TODO: inform coordinator that we couldn't connect with the worker so coordinator
			// can mark the worker as failed
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

		tempFile.Close()
	}
}
