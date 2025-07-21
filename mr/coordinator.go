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

	c.server()
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
		log.Println("All map tasks are complete. Assigning a reduce task.")
		for _, task := range c.Tasks {
			if task.Type == Reduce && task.State == Idle {
				task.WorkerAddr = args.WorkerAddr
				task.State = InProgress

				outputLocations := make([]MapOutputLocation, 0)
				for _, output := range c.IntermediateFiles {
					address := c.Workers[output.WorkerAddr].Address
					outputLocations = append(outputLocations, MapOutputLocation{
						TaskID:        output.TaskID,
						PartID:        task.ID,
						WorkerAddress: address,
					})
				}

				reply.TaskID = task.ID
				reply.MapOutputLocations = outputLocations
				reply.TaskType = task.Type
				return nil
			}
		}
	}

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
		}
	}

	return allDone
}

func (c *Coordinator) MapTaskComplete(args *MapTaskCompleteArgs, reply *MapTaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, err := c.GetTaskByID(args.TaskID)
	if err != nil {
		reply.Success = false
		return err
	}

	task.State = Completed

	c.IntermediateFiles = append(c.IntermediateFiles, &MapTaskOutput{
		WorkerAddr: args.WorkerAddr,
		TaskID:     args.TaskID,
		Filenames:  args.IntermediateFiles,
	})

	log.Printf("Worker %d completed Map task %d", args.WorkerAddr, args.TaskID)
	reply.Success = true

	return nil
}

func (c *Coordinator) GetTaskByID(id int) (*Task, error) {
	for _, task := range c.Tasks {
		if task.ID == id {
			return task, nil
		}
	}

	return nil, fmt.Errorf("Task not found")
}

func (c *Coordinator) ReportHeartbeat(args *ReportHeartbeatArgs, reply *ReportHeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Workers[args.WorkerAddr].LastSeen = time.Now()

	return nil
}

func (c *Coordinator) checkHeartbeat() {
	for {
		for workerAddr, worker := range c.Workers {
			// failure of worker machine to send heartbeat
			if worker.LastSeen.Add(6 * time.Second).After(time.Now()) {
				c.makeAllTaskIdle(workerAddr)
				delete(c.Workers, workerAddr)
			}
		}
		time.Sleep(1)
	}
}

// convert all in-progress and completed task by workerAddr to idle
func (c *Coordinator) makeAllTaskIdle(workerAddr string) {
	for _, task := range c.Tasks {
		if task.WorkerAddr == workerAddr {
			task.State = Idle
		}
	}
}
