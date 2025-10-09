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
	Index int
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
				task.mu.Lock()
				task.State = TaskInProgress
				task.CreatedAt = time.Now()
				task.mu.Unlock()
				
				resp.TaskType = TaskTypeMap
				resp.TaskID = task.TaskID
				resp.File = task.Filename
				resp.NReduce = c.NReduce
				resp.NMap = len(c.MapTasks)

				fmt.Fprintf(os.Stderr, "[Coordinator]: Assigned map task %v\n", task.TaskID)
				go c.monitorTask(task)

			return nil
			}
			if c.allDone(c.MapTasks) {
				c.Step = ReduceStep
				fmt.Fprintf(os.Stderr, "[Coordinator]: All map tasks done, advancing to ReduceStep\n")
			}
		
		case ReduceStep:
			task := c.findIdleTask(c.ReduceTasks)
			if task != nil {
				task.mu.Lock()
				task.State = TaskInProgress
				task.CreatedAt = time.Now()
				task.mu.Unlock()
				
				resp.TaskType = TaskTypeReduce
				resp.TaskID = task.TaskID
				resp.File = task.Filename
				resp.NReduce = c.NReduce
				resp.NMap = len(c.MapTasks)
				resp.ReduceIndex = task.Index

				fmt.Fprintf(os.Stderr, "[Coordinator]: Assigned reduce task %v\n", task.TaskID)
				
				go c.monitorTask(task)
			return nil
		}
	} 
	return nil
}

func (c *Coordinator) allDone(tasks []*Task) bool {
	for _, t := range tasks {
		t.mu.Lock()
		state := t.State != TaskDone
		t.mu.Unlock()
		if state {
			return false
		}
	}
	return true
}

func (c *Coordinator) HandleTaskDone(req *TaskDoneRequest, resp *TaskDoneResponse) error {
	fmt.Fprintf(os.Stderr, "[Coordinator]: Received HandleTaskDone for task %v\n", req.TaskID)
	c.mu.Lock()
	var task *Task

	for _, t := range c.MapTasks {
		if t.TaskID == req.TaskID {
			task = t 
			break
		}
	}

	if task == nil {
		for _, t := range c.ReduceTasks {
			if t.TaskID == req.TaskID {
				task = t
				break
			}
		}
	}
	c.mu.Unlock()


    if task == nil {
        fmt.Fprintf(os.Stderr, "[Coordinator]: task %v not found\n", req.TaskID)
        return nil
    }

	task.mu.Lock()
	if time.Since(task.CreatedAt) > 10*time.Second {
		fmt.Fprintf(os.Stderr, "[Coordinator]: task %v reported too late, ignoring\n", task.TaskID)
		return nil
	}

	task.State = TaskDone
	task.mu.Unlock()

	c.mu.Lock()
	if c.Step == MapStep && c.allDone(c.MapTasks) {
		c.Step = ReduceStep
		fmt.Fprintf(os.Stderr, "[Coordinator]: All map tasks done, advancing to ReduceStep\n")
	}
	c.mu.Unlock()
	return nil

}

func (c *Coordinator) findIdleTask(tasks []*Task) *Task {
	for _, t := range tasks {
		t.mu.Lock()
		idle := t.State == TaskIdle
		t.mu.Unlock()
		if idle {
			return t
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
	
	ret = c.allDone(c.MapTasks) && c.allDone(c.ReduceTasks)


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
	c.NReduce = nReduce

	for i, f := range files {
		c.MapTasks[i] = &Task{
			TaskID: fmt.Sprintf("m-%d", i),
			Filename: f,
			State: TaskIdle,
			TaskType: TaskTypeMap,
			Index: i,
		}
	}

	for i:= 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &Task{
			TaskID: fmt.Sprintf("r-%d", i),
			State: TaskIdle,
			TaskType: TaskTypeMap,
			Index: i,
		}
	}

	c.server()
	return &c
}
