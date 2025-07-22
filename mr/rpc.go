package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)

// AssignTask
type MapOutputLocation struct {
	TaskID        int
	PartID        int
	WorkerAddress string
}

type AssignTaskArgs struct {
	WorkerAddr string
}

type AssignTaskReply struct {
	TaskID             int
	TaskType           TaskType
	TaskFile           string              // map
	PartitionCount     int                 // map
	ReduceID           int                 // reduce
	MapOutputLocations []MapOutputLocation // reduce
}

// MapTaskComplete
type ReportMapStatusArgs struct {
	WorkerAddr        string
	TaskID            int
	IntermediateFiles []string
	Status            Status
	Error             string
}

type ReportMapStatusReply struct {
}

// ReduceTaskComplete
type ReportReduceStatusArgs struct {
	TaskID int
	Status Status
	Error  string
}

type ReportReduceStatusReply struct {
}

// GetIntermediateFiles
type GetIntermediateFileArgs struct {
	Filename string
}

type GetIntermediateFileReply struct {
	Content []byte
}

// ReportHeartbeat
type ReportHeartbeatArgs struct {
	WorkerAddr string
}

type ReportHeartbeatReply struct {
}

// Shutdown Worker
type ShutdownWorkerArgs struct {
}

type ShutdownWorkerReply struct {
}

func coordinatorSock() string {
	return fmt.Sprintf("/var/tmp/mr-%d", os.Getuid())
}

func workerSock() string {
	return fmt.Sprintf("/var/tmp/mr-%d.sock", os.Getpid())
}

func call(rpcname string, args any, reply any, sockname string) bool {
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if nil == err {
		return true
	}

	log.Println("RPC Call Error: ", err)
	return false
}
