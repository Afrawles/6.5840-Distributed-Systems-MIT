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


type Coordinator struct {
	// Your definitions here.
	MapTasks []*Task
	ReduceTasks []*Task

	Step int
	NReduce int
	mu sync.Mutex
}


const (
	TaskIdle int = iota
	TaskInProgress
	TaskDone
)

const (
	MapStep int = iota
	ReduceStep
)

type Task struct {
	TaskID string
	TaskType int
	State int
	Filename string
	CreatedAt time.Time
	mu sync.Mutex
}

func (c *Coordinator) monitorTask(task *Task) {
	time.Sleep(10 * time.Second)
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.State == TaskDone {
		fmt.Fprintf(os.Stderr, "[Coordinator]: task %v Done", task.TaskID)
	} else {
		task.State = TaskIdle
		fmt.Fprintf(os.Stderr, "[Coordinator]: task %v Failed Ready for processing", task.TaskID)
	}
} 

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTaskRequest(req *TaskRequest, resp *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.Step {
		case MapStep:
			task := c.findIdleTask(c.MapTasks)
			if task != nil {
				task.State = TaskInProgress
				task.CreatedAt = time.Now()
				
				resp.TaskType = TaskTypeMap
				resp.TaskID = task.TaskID
				resp.File = task.Filename
				resp.NReduce = c.NReduce

				go c.monitorTask(task)

			return nil
		} 
		
		case ReduceStep:
			task := c.findIdleTask(c.ReduceTasks)
			if task != nil {
				task.State = TaskInProgress
				task.CreatedAt = time.Now()
				
				resp.TaskType = TaskTypeReduce
				resp.TaskID = task.TaskID
				resp.File = task.Filename
				resp.NReduce = c.NReduce
				
				go c.monitorTask(task)
			return nil
		}
	} 
	return nil
}

func (c *Coordinator) HandleTaskDone(req *TaskDoneRequest, resp *TaskDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task


	for i := range c.MapTasks {
		if c.MapTasks[i].TaskID == req.TaskID {
			task = c.MapTasks[i]
			break
		}
	}

	if task == nil {
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].TaskID == req.TaskID {
				task = c.ReduceTasks[i]
				break
			}
		}
	}

    if task == nil {
        fmt.Fprintf(os.Stderr, "[Coordinator]: task %v not found\n", req.TaskID)
        return nil
    }

	if time.Since(task.CreatedAt) > 10*time.Minute {
		fmt.Fprintf(os.Stderr, "[Coordinator]: task %v reported too late, ignoring\n", task.TaskID)
		return nil
	}

	task.State = TaskDone
	return nil

}

func (c *Coordinator) findIdleTask(tasks []*Task) *Task {
	for i := range tasks {
		if tasks[i].State == TaskIdle {
			return tasks[i]
		}
	} 
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for _, t := range c.MapTasks {
		t.mu.Lock()
		if t.State != TaskDone {
			return false
		}
		t.mu.Unlock()
	}

	for _, t := range c.ReduceTasks {
		t.mu.Lock()
		if t.State != TaskDone {
			return false
		}
		t.mu.Unlock()
	}

	ret = true

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	
	c.MapTasks = make([]*Task, len(files))
	c.ReduceTasks = make([]*Task, nReduce)
	c.Step = MapStep

	for i, f := range files {
		c.MapTasks[i] = &Task{
			TaskID: fmt.Sprintf("m-%d", i),
			Filename: f,
			State: TaskIdle,
		}
	}

	for i:= 0; i <= nReduce; i++ {
		c.ReduceTasks[i] = &Task{
			TaskID: fmt.Sprintf("r-%d", i),
			State: TaskIdle,
		}
	}

	c.server()
	return &c
}
