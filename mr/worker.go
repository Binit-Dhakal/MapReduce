package mr

import (
	"context"
	"fmt"
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
	cancel   context.CancelFunc

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

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	go w.pingCoordinator(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
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

func (w *Worker) pingCoordinator(ctx context.Context) {
	coordinatorSockName := coordinatorSock()

	ticker := time.NewTicker(2 * time.Second)
	args := &ReportHeartbeatArgs{
		WorkerAddr: w.Address,
	}
	reply := &ReportHeartbeatReply{}

	for {
		select {
		case <-ticker.C:
			call("Coordinator.ReportHeartbeat", args, reply, coordinatorSockName)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) ShutdownWorker(args *ShutdownWorkerArgs, reply *ShutdownWorkerReply) error {
	fmt.Println("Shutdown signal received")
	w.cancel()
	return nil
}
