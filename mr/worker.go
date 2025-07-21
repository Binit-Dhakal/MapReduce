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
	ID       int
	Address  string
	LastSeen time.Time

	MapWorkerLogic
}

func NewWorker(mapf MapFunc, reducef ReduceFunc) {
	w := &Worker{}
	sockname := w.startWorkerServer()

	coordinatorSockName := coordinatorSock()

	args := RegisterWorkerArgs{
		Address: sockname,
	}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply, coordinatorSockName)
	if ok {
		log.Printf("Worker registered, Id: %d", reply.WorkerID)
	} else {
		return
	}
	workerID := reply.WorkerID

	// poll the server to check for if there are any tasks
	assignArgs := AssignTaskArgs{
		WorkerID: workerID,
	}
	assignReply := AssignTaskReply{}

	ok = call("Coordinator.AssignTask", &assignArgs, &assignReply, coordinatorSockName)
	if !ok {
		log.Println("Not assigned any task")
	}

	taskID := assignReply.TaskID
	partitionCount := assignReply.PartitionCount

	switch assignReply.TaskType {
	case Map:
		mapWorker := NewMapWorker(
			workerID, assignReply.TaskFile, taskID, partitionCount, mapf,
		)
		mapWorker.ExecuteMap()

	case Reduce:
		reduceWorker := NewReduceWorker(
			workerID, assignReply.TaskID, reducef, assignReply.MapOutputLocations,
		)
		reduceWorker.ExecuteReduce()

	default:
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

func (w *Worker) pingCoordinator(workerID int) {
	coordinatorSockName := coordinatorSock()

	ticker := time.NewTicker(2 * time.Second)
	args := &ReportHeartbeatArgs{
		WorkerID: workerID,
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
