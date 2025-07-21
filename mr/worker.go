package mr

import (
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

	MapWorkerLogic
}

func NewWorker(mapf MapFunc, reducef ReduceFunc) {
	w := &Worker{}
	sockname := w.startWorkerServer()

	coordinatorSockName := coordinatorSock()

	// poll the server to check for if there are any tasks
	assignArgs := AssignTaskArgs{
		WorkerAddr: sockname,
	}
	assignReply := AssignTaskReply{}

	go w.pingCoordinator(sockname)

	for {
		ok := call("Coordinator.AssignTask", &assignArgs, &assignReply, coordinatorSockName)
		if !ok {
			log.Println("Not assigned any task")
		}

		taskID := assignReply.TaskID
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

		default:
		}
		time.Sleep(5 * time.Second)
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

func (w *Worker) pingCoordinator(workerAddr string) {
	coordinatorSockName := coordinatorSock()

	ticker := time.NewTicker(2 * time.Second)
	args := &ReportHeartbeatArgs{
		WorkerAddr: workerAddr,
	}
	reply := &ReportHeartbeatReply{}

	for {
		select {
		case <-ticker.C:
			call("Coordinator.ReportHeartBeat", args, reply, coordinatorSockName)
		}
	}
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
