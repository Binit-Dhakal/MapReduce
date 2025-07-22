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
				// try again
				time.Sleep(3 * time.Second)
				continue
			}
			fmt.Printf("Assigned Task- %v, ID-%d\n", assignReply.TaskType, assignReply.TaskID)

			partitionCount := assignReply.PartitionCount

			switch assignReply.TaskType {
			case Map:
				time.Sleep(1 * time.Second)

				mapWorker := NewMapWorker(
					assignReply.TaskFile, taskID, partitionCount, mapf,
				)
				args := &ReportMapStatusArgs{
					WorkerAddr: sockname,
					TaskID:     taskID,
					Status:     Completed,
				}

				reply := &ReportMapStatusReply{}
				intermediateFiles, err := mapWorker.ExecuteMap()
				if err != nil {
					args.Status = Failed
					args.Error = err.Error()
				} else {
					args.IntermediateFiles = intermediateFiles
				}
				call("Coordinator.ReportMapStatus", args, reply, coordinatorSockName)

			case Reduce:
				reduceWorker := NewReduceWorker(
					assignReply.TaskID, assignReply.ReduceID, reducef,
				)
				args := &ReportReduceStatusArgs{
					TaskID: taskID,
					Status: Completed,
				}
				reply := &ReportMapStatusReply{}
				time.Sleep(2 * time.Second)

				err := reduceWorker.ExecuteReduce()
				if err != nil {
					args.Status = Failed
					args.Error = err.Error()
				}

				call("Coordinator.ReportReduceStatus", args, reply, coordinatorSock())
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

	args := &ReportHeartbeatArgs{
		WorkerAddr: w.Address,
	}
	reply := &ReportHeartbeatReply{}

	call("Coordinator.ReportHeartbeat", args, reply, coordinatorSockName)

	ticker := time.NewTicker(2 * time.Second)
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
