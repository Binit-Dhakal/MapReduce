package mr

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
)

type MapWorker struct {
	taskFile            string
	taskID              int
	partitionCount      int
	workerID            int
	mapfunc             MapFunc
	coordinatorSockName string
}

func NewMapWorker(workerID int, taskfile string, taskID int, partitionCount int, mapper MapFunc) *MapWorker {
	coordinatorSockName := coordinatorSock()
	return &MapWorker{
		taskFile:            taskfile,
		taskID:              taskID,
		partitionCount:      partitionCount,
		workerID:            workerID,
		mapfunc:             mapper,
		coordinatorSockName: coordinatorSockName,
	}
}

// 1. Run map function with taskfile
// 2. Sort the intermediate key-value before saving
// 3. Save the intermediate files locally
// 4. Send MapTaskComplete rpc request to coordinator
func (m *MapWorker) ExecuteMap() {
	intermediate := m.mapfunc(m.taskFile, "")
	// Sort before saving to disk
	// cseweb.ucsd.edu/classes/sp16/cse291-e/applications/ln/lecture14.html
	sort.Sort(ByKey(intermediate))

	m.saveIntermediate(intermediate, m.taskID, m.partitionCount)

	intermediateFiles := make([]string, m.partitionCount)
	for i := range m.partitionCount {
		filename := fmt.Sprintf("task-%d-part-%d", m.taskID, i)
		intermediateFiles[i] = filename
	}

	completeArgs := &MapTaskCompleteArgs{
		WorkerID:          m.workerID,
		TaskID:            m.taskID,
		IntermediateFiles: intermediateFiles,
	}
	completeReply := &MapTaskCompleteReply{}

	call("Coordinator.MapTaskComplete", completeArgs, completeReply, m.coordinatorSockName)
	if !completeReply.Success {
		log.Println("MapTaskComplete Error")
	}
}

func (m *MapWorker) saveIntermediate(pairs []KeyValue, taskID int, nPartition int) {
	intermediateFiles := make([]*os.File, nPartition)

	for i := range nPartition {
		filename := fmt.Sprintf("task-%d-part-%d", taskID, i)

		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to open intermediate file: %v", file)
		}
		intermediateFiles[i] = file
	}

	defer func() {
		for _, file := range intermediateFiles {
			if file != nil {
				file.Close()
			}
		}
	}()

	for _, pair := range pairs {
		partition := ihash(pair.Key) % nPartition

		_, err := fmt.Fprintf(intermediateFiles[partition], "%s %s\n", pair.Key, pair.Value)
		if err != nil {
			log.Fatalf("Failed to write to intermediate file: %v", err)
		}
	}
}

// RPC function to get intermediate file (from reduceWorker to mapWorker)
func (mw *MapWorkerLogic) GetIntermediateFile(args *GetIntermediateFileArgs, reply *GetIntermediateFileReply) error {
	f, err := os.Open(args.Filename)
	if err != nil {
		return err
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	reply.Content = content
	return nil
}
