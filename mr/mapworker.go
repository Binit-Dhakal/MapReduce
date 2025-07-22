package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
)

type MapWorker struct {
	taskFile       string
	taskID         int
	totalPartition int
	mapfunc        MapFunc
}

func NewMapWorker(taskfile string, taskID int, totalPartition int, mapper MapFunc) *MapWorker {
	return &MapWorker{
		taskFile:       taskfile,
		taskID:         taskID,
		totalPartition: totalPartition,
		mapfunc:        mapper,
	}
}

// 1. Run map function with taskfile
// 2. Sort the intermediate key-value before saving
// 3. Save the intermediate files locally
// 4. Send MapTaskComplete rpc request to coordinator
func (m *MapWorker) ExecuteMap() ([]string, error) {
	fileSuffix := "/tmp/mr_map/%d/task-%d"

	for i := range m.totalPartition {
		tempDir := fmt.Sprintf("/tmp/mr_map/%d", i)
		os.MkdirAll(tempDir, 0755)
	}

	contentBytes, err := os.ReadFile(m.taskFile)
	if err != nil {
		return nil, fmt.Errorf("Error reading file: %v\n", err)
	}

	fileContent := string(contentBytes)
	intermediate := m.mapfunc(m.taskFile, fileContent)

	// Sort before saving to disk
	// cseweb.ucsd.edu/classes/sp16/cse291-e/applications/ln/lecture14.html
	sort.Sort(ByKey(intermediate))

	err = m.saveIntermediate(intermediate, m.taskID, m.totalPartition, fileSuffix)
	if err != nil {
		return nil, err
	}

	intermediateFiles := make([]string, m.totalPartition)
	for i := range m.totalPartition {
		filename := fmt.Sprintf(fileSuffix, i, m.taskID)
		intermediateFiles[i] = filename
	}

	return intermediateFiles, nil
}

func (m *MapWorker) saveIntermediate(pairs []KeyValue, taskID int, nPartition int, suffix string) error {
	intermediateFiles := make([]*os.File, nPartition)

	for i := range nPartition {
		filename := fmt.Sprintf(suffix, i, taskID)

		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("Failed to open intermediate file: %v", file)
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
			return fmt.Errorf("Failed to write to intermediate file: %v", err)
		}
	}

	return nil
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
