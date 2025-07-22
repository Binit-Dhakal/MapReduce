package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status int
type TaskType int

const (
	Idle Status = iota
	InProgress
	Completed
)

const (
	Map TaskType = iota
	Reduce
)

type Task struct {
	ID         int
	Type       TaskType
	Input      string
	State      Status
	WorkerAddr string // for non-idle task
}

type MapTaskOutput struct {
	TaskID     int
	WorkerAddr string
	Filenames  []string
}

type Coordinator struct {
	Tasks             []*Task            // all tasks
	IntermediateFiles []*MapTaskOutput   // all results from map
	Workers           map[string]*Worker // active workers
	ReduceNum         int
	mu                sync.Mutex
}

func NewCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		Tasks:             make([]*Task, 0),
		IntermediateFiles: make([]*MapTaskOutput, 0),
		Workers:           map[string]*Worker{},
		ReduceNum:         nReduce,
	}

	// map tasks
	for i, file := range files {
		c.Tasks = append(c.Tasks, &Task{
			ID:    i,
			Type:  Map,
			Input: file,
			State: Idle,
		})
	}

	// reduce tasks
	for i := range nReduce {
		c.Tasks = append(c.Tasks, &Task{
			ID:    i,
			Type:  Reduce,
			State: Idle,
		})
	}

	for _, task := range c.Tasks {
		fmt.Printf("Created task: %v\n", task)
	}

	c.server()

	go c.checkHeartbeat()

	return c
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.Tasks {
		if task.Type == Map && task.State == Idle {
			task.WorkerAddr = args.WorkerAddr
			task.State = InProgress

			reply.TaskID = task.ID
			reply.TaskFile = task.Input
			reply.TaskType = task.Type
			reply.PartitionCount = c.ReduceNum

			fmt.Printf(
				"Assigned task %d with file %v of type %v to %s\n",
				task.ID, task.Input, task.Type, args.WorkerAddr,
			)
			return nil
		}
	}

	allMapsCompleted := true
	for _, task := range c.Tasks {
		if task.Type == Map && task.State != Completed {
			allMapsCompleted = false
			break
		}
	}

	// we assign reduce task if all map task is completed
	if allMapsCompleted {
		for _, task := range c.Tasks {
			if task.Type != Reduce || task.State != Idle {
				continue
			}
			task.WorkerAddr = args.WorkerAddr
			task.State = InProgress

			outputLocations := make([]MapOutputLocation, 0)
			for _, output := range c.IntermediateFiles {
				outputLocations = append(outputLocations, MapOutputLocation{
					TaskID:        output.TaskID,
					PartID:        task.ID,
					WorkerAddress: output.WorkerAddr,
				})
			}

			reply.TaskID = task.ID
			reply.MapOutputLocations = outputLocations
			reply.TaskType = task.Type

			fmt.Printf("Assigned task %d with file %v of type %v to %s\n", task.ID, task.Input, task.Type, args.WorkerAddr)
			return nil
		}
	}

	// to indicate we have not assigned worker any task now
	reply.TaskID = -1
	return nil
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
		for workerAddr, _ := range c.Workers {
			reply := &ShutdownWorkerReply{}
			call("Worker.ShutdownWorker", args, reply, workerAddr)
		}
	}

	return allDone
}

func (c *Coordinator) MapTaskComplete(args *MapTaskCompleteArgs, reply *MapTaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	notFound := true
	for _, task := range c.Tasks {
		if task.ID == args.TaskID && task.Type == Map {
			task.State = Completed
			notFound = false
		}
	}
	if notFound {
		reply.Success = false
		return fmt.Errorf("Task not found")
	}

	c.IntermediateFiles = append(c.IntermediateFiles, &MapTaskOutput{
		WorkerAddr: args.WorkerAddr,
		TaskID:     args.TaskID,
		Filenames:  args.IntermediateFiles,
	})

	log.Printf("Worker %s completed Map task %d", args.WorkerAddr, args.TaskID)
	reply.Success = true

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

func (c *Coordinator) checkHeartbeat() {
	for {
		c.mu.Lock()
		for workerAddr, worker := range c.Workers {
			// failure of worker machine to send heartbeat
			if time.Since(worker.LastSeen) > 6*time.Second {
				// convert all in-progress and completed task by workerAddr to idle
				fmt.Printf("Deleting all task of worker %s\n\n", workerAddr)
				for _, task := range c.Tasks {
					if task.WorkerAddr == workerAddr {
						task.State = Idle
					}
				}
				delete(c.Workers, workerAddr)
			}
		}
		c.mu.Unlock()
		time.Sleep(2)
	}
}

func (c *Coordinator) ReduceTaskReport(args *ReduceTaskReportArgs, reply *ReduceTaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("Reduce Task completed: %+v", args)
	if args.Status == Completed {
		for _, task := range c.Tasks {
			if task.Type == Reduce && task.ID == args.TaskID {
				task.State = Completed
				fmt.Printf("Reduce Task completed: %+v", task)
			}
		}
	}

	reply.Success = true
	return nil
}
