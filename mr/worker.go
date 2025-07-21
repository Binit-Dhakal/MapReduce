package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Worker struct {
	Address  string // identity of worker machine
	LastSeen time.Time
	assigned bool

	MapWorkerLogic
}

func NewWorker(mapf MapFunc, reducef ReduceFunc) {
	w := &Worker{}
	sockname := w.startWorkerServer()
	w.Address = sockname

	coordinatorSockName := coordinatorSock()

	// poll the server to check for if there are any tasks
	assignArgs := AssignTaskArgs{
		WorkerAddr: sockname,
	}

	go w.pingCoordinator()

	for {
		assignReply := AssignTaskReply{}
		ok := call("Coordinator.AssignTask", &assignArgs, &assignReply, coordinatorSockName)
		if !ok {
			log.Println("Not assigned any task")
			continue
		}

		taskID := assignReply.TaskID
		if taskID == -1 {
			time.Sleep(3 * time.Second)
			continue
		}
		fmt.Printf("Assigned Task- %v, ID-%d\n", assignReply.TaskType, assignReply.TaskID)

		partitionCount := assignReply.PartitionCount

		switch assignReply.TaskType {
		case Map:
			mapWorker := NewMapWorker(
				sockname, assignReply.TaskFile, taskID, partitionCount, mapf,
			)
			mapWorker.ExecuteMap()

		case Reduce:
			reduceWorker := NewReduceWorker(
				sockname, assignReply.TaskID, reducef, assignReply.MapOutputLocations,
			)
			reduceWorker.ExecuteReduce()

		}
	}
}

func (w *Worker) startWorkerServer() string {
	rpc.Register(w)
	rpc.HandleHTTP()

	sockName := workerSock()
	os.Remove(sockName)
	l, err := net.Listen("unix", sockName)
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	go http.Serve(l, nil)
	return sockName
}

func (w *Worker) pingCoordinator() {
	coordinatorSockName := coordinatorSock()

	ticker := time.NewTicker(2 * time.Second)
	args := &ReportHeartbeatArgs{
		WorkerAddr: w.Address,
	}
	reply := &ReportHeartbeatReply{}

	for range ticker.C {
		call("Coordinator.ReportHeartbeat", args, reply, coordinatorSockName)
	}
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
