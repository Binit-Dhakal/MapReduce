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
	ID        int
	Type      TaskType
	Input     string
	State     Status
	MachineID int
}

type MapTaskOutput struct {
	TaskID    int
	WorkerID  int
	Filenames []string
}

type Coordinator struct {
	Tasks             []*Task
	IntermediateFiles []*MapTaskOutput
	Workers           map[int]*Worker
	NextWorkerID      int
	ReduceNum         int
	CurrReduceNum     int
	mu                sync.Mutex
}

func NewCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		Tasks:             make([]*Task, 0),
		IntermediateFiles: make([]*MapTaskOutput, 0),
		Workers:           map[int]*Worker{},
		NextWorkerID:      0,
		CurrReduceNum:     0,
		ReduceNum:         nReduce,
	}

	// map tasks
	for i, file := range files {
		c.Tasks = append(c.Tasks, &Task{
			ID:        i,
			Type:      Map,
			Input:     file,
			State:     Idle,
			MachineID: 0,
		})
	}

	// reduce tasks
	for i := range nReduce {
		c.Tasks = append(c.Tasks, &Task{
			ID:        i,
			Type:      Reduce,
			State:     Idle,
			MachineID: 0,
		})
	}

	c.server()
	return c
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerID := args.WorkerID

	for _, task := range c.Tasks {
		if task.Type == Map && task.State == Idle {
			task.MachineID = workerID
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

	// if all maps are completed then we will start assigning reduce task
	// although assigning reduce task only after all reduce task is kinda slow
	if allMapsCompleted {
		// need to assign reduce task
		log.Println("All map tasks are complete. Assigning a reduce task.")
		for _, task := range c.Tasks {
			if task.Type == Reduce && task.State == Idle {
				task.MachineID = workerID
				task.State = InProgress

				outputLocations := make([]MapOutputLocation, 0)
				for _, output := range c.IntermediateFiles {
					address := c.Workers[output.WorkerID].Address
					outputLocations = append(outputLocations, MapOutputLocation{
						TaskID:        output.TaskID,
						PartID:        task.ID,
						WorkerAddress: address,
					})
				}

				reply.TaskID = task.ID
				reply.MapOutputLocations = outputLocations
				reply.TaskType = task.Type
				c.CurrReduceNum++
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

// Register worker to our coordinator
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerID := c.NextWorkerID

	c.Workers[workerID] = &Worker{
		ID:       c.NextWorkerID,
		Address:  args.Address,
		LastSeen: time.Now(),
	}

	reply.WorkerID = workerID
	c.NextWorkerID++

	return nil
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
		TaskID:    args.TaskID,
		WorkerID:  args.WorkerID,
		Filenames: args.IntermediateFiles,
	})

	// to not check worker
	delete(c.Workers, args.WorkerID)

	log.Printf("Worker %d completed Map task %d", args.WorkerID, args.TaskID)
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

	c.Workers[args.WorkerID].LastSeen = time.Now()

	return nil

}
