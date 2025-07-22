package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type Status int
type TaskType int

const (
	Idle Status = iota
	InProgress
	Completed
	Failed
)

const (
	Map TaskType = iota
	Reduce
)

type Task struct {
	ID         int
	Type       TaskType
	State      Status
	WorkerAddr string // for non-idle task

	Input    string // map
	ReduceID int    // reduce
}

type MapTaskOutput struct {
	TaskID     int
	WorkerAddr string
	Filenames  []string
}

type Coordinator struct {
	Tasks             map[int]*Task       // all tasks
	IntermediateFiles map[string][]string // all results from map
	Workers           map[string]*Worker  // active workers

	// to check which phase our program is in: Map or Reduce phase
	NumFinishMapTask    int
	NumFinishReduceTask int
	nReduce             int
	nFile               int

	mapTaskChan    chan *Task
	reduceTaskChan chan *Task
	cancel         context.CancelFunc
	mu             sync.Mutex
}

func NewCoordinator(files []string, nReduce int) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Coordinator{
		Tasks:             map[int]*Task{},
		IntermediateFiles: map[string][]string{},
		Workers:           map[string]*Worker{},

		NumFinishMapTask:    0,
		NumFinishReduceTask: 0,
		nReduce:             nReduce,
		nFile:               len(files),

		mapTaskChan:    make(chan *Task, len(files)),
		reduceTaskChan: make(chan *Task, nReduce),
		cancel:         cancel,
	}

	// map tasks
	for i, file := range files {
		task := &Task{
			ID:    i,
			Type:  Map,
			Input: file,
			State: Idle,
		}
		c.Tasks[task.ID] = task
		c.mapTaskChan <- task
	}

	// reduce tasks
	for i := range nReduce {
		task := &Task{
			ID:       len(files) + i,
			Type:     Reduce,
			State:    Idle,
			ReduceID: i,
		}
		c.Tasks[task.ID] = task
		c.reduceTaskChan <- task
	}

	for _, task := range c.Tasks {
		fmt.Printf("Created task: %v\n", task)
	}

	c.server()

	go c.checkHeartbeat(ctx)

	return c
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockName := coordinatorSock()
	os.Remove(sockName)

	l, err := net.Listen("unix", sockName)
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	go http.Serve(l, nil)
}

// main function continously polls if job is completed
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	allDone := true
	for _, task := range c.Tasks {
		if task.State != Completed {
			allDone = false
			break
		}
	}

	if allDone {
		args := &ShutdownWorkerArgs{}
		for workerAddr := range c.Workers {
			reply := &ShutdownWorkerReply{}
			call("Worker.ShutdownWorker", args, reply, workerAddr)
		}
	}

	return allDone
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println(c.NumFinishMapTask, c.nFile)
	if c.NumFinishMapTask < c.nFile {
		select {
		case task := <-c.mapTaskChan:
			task.State = InProgress
			task.WorkerAddr = args.WorkerAddr

			reply.TaskID = task.ID
			reply.TaskFile = task.Input
			reply.TaskType = task.Type
			reply.PartitionCount = c.nReduce

			fmt.Printf(
				"Assigned task %d with file %v of type %v to %s\n",
				task.ID, task.Input, task.Type, args.WorkerAddr,
			)
			return nil
		default:
			reply.TaskID = -1 // no map task left to assign for now
			return nil
		}
	}

	if c.NumFinishReduceTask < c.nReduce {
		select {
		case task := <-c.reduceTaskChan:
			task.State = InProgress
			task.WorkerAddr = args.WorkerAddr

			reply.TaskID = task.ID
			reply.TaskType = task.Type
			reply.ReduceID = task.ReduceID
			outputLocations := make(map[string][]string, 0)
			for address, filenames := range c.IntermediateFiles {
				for _, filename := range filenames {
					pattern := fmt.Sprintf("/tmp/mr_map/%d/", task.ReduceID)
					if strings.Contains(filename, pattern) {
						outputLocations[address] = append(outputLocations[address], filename)
					}
				}
			}

			reply.MapOutputLocations = outputLocations
			fmt.Printf(
				"Assigned task %d with file %v of type %v to %s\n",
				task.ID, task.Input, task.Type, args.WorkerAddr,
			)
			return nil
		default:
			reply.TaskID = -1 // no reduce task left to assign for now
			return nil
		}
	}

	// to indicate we have not assigned worker any task now
	reply.TaskID = -1
	return nil
}

func (c *Coordinator) checkHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			for workerAddr, worker := range c.Workers {
				// failure of worker machine to send heartbeat
				if time.Since(worker.LastSeen) > 6*time.Second {
					// convert all in-progress and completed task by workerAddr to idle
					fmt.Printf("Worker not responding so deleting all task of worker %s\n\n", workerAddr)
					for _, task := range c.Tasks {
						if task.WorkerAddr == workerAddr {
							if task.State == Completed {
								c.NumFinishMapTask--
							}
							task.State = Idle
							c.mapTaskChan <- task
						}
					}
					delete(c.Workers, workerAddr)
					delete(c.IntermediateFiles, workerAddr)
				}
			}
			c.mu.Unlock()
		case <-ctx.Done():
			break
		}
	}
}

func (c *Coordinator) ReportMapStatus(args *ReportMapStatusArgs, reply *ReportMapStatusReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, ok := c.Tasks[args.TaskID]
	if !ok {
		return fmt.Errorf("Task not found")
	}

	if task.State == Failed {
		task.State = Idle
		c.mapTaskChan <- task
		fmt.Println(args.Error)
		return nil
	}

	task.State = Completed

	c.IntermediateFiles[args.WorkerAddr] = append(
		c.IntermediateFiles[args.WorkerAddr],
		args.IntermediateFiles...,
	)

	c.NumFinishMapTask++
	return nil
}

func (c *Coordinator) ReportHeartbeat(args *ReportHeartbeatArgs, reply *ReportHeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println("Heartbeat: ", args.WorkerAddr)
	if _, exists := c.Workers[args.WorkerAddr]; !exists {
		c.Workers[args.WorkerAddr] = &Worker{
			Address: args.WorkerAddr,
		}
	}

	c.Workers[args.WorkerAddr].LastSeen = time.Now()

	return nil
}

func (c *Coordinator) ReportReduceStatus(args *ReportReduceStatusArgs, reply *ReportReduceStatusReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, ok := c.Tasks[args.TaskID]
	if !ok {
		return fmt.Errorf("Task not found")
	}

	if args.Status == Failed {
		task.State = Idle
		c.reduceTaskChan <- task
		return nil
	}

	task.State = Completed

	c.NumFinishReduceTask++

	return nil
}
