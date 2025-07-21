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
	TaskFile           string // map
	TaskType           TaskType
	PartitionCount     int                 // map
	MapOutputLocations []MapOutputLocation // reduce
}

// MapTaskComplete
type MapTaskCompleteArgs struct {
	WorkerAddr        string
	TaskID            int
	IntermediateFiles []string
}

type MapTaskCompleteReply struct {
	Success bool
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

func coordinatorSock() string {
	return fmt.Sprintf("/var/tmp/mr-%d", os.Getuid())
}

func workerSock() string {
	return fmt.Sprintf("/var/tmp/mr-%d.sock", os.Getpid())
}

func call(rpcname string, args any, reply any, sockname string) bool {
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if nil == err {
		return true
	}

	log.Println(err)
	return false
}
