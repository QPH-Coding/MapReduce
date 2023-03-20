package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	stage     string
	nMap      int
	nReduce   int
	m         sync.Mutex
	tasks     map[string]Task
	tasksChan chan Task
}

// RequestTask
// RPC Handler: Worker requests Task (and commit finish work)
func (c *Coordinator) RequestTask(req *Request, resp *Response) error {
	switch req.LastTaskType {
	case "MAP":
		c.mapDoneCheck(req)
	case "REDUCE":
		c.reduceDoneCheck(req)
	}

	task, ok := <-c.tasksChan
	if !ok {
		return nil
	}
	c.m.Lock()
	defer c.m.Unlock()
	task.WorkerId = req.WorkerId
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[generateTaskId(task.Type, task.Index)] = task
	resp.MapNum = c.nMap
	resp.ReduceNum = c.nReduce
	resp.Task = Task{
		Type:         task.Type,
		Index:        task.Index,
		WorkerId:     task.WorkerId,
		MapInputFile: task.MapInputFile,
	}
	//fmt.Println("Task", generateTaskId(task.Type, task.Index), "apply to WORKER", req.WorkerId)
	return nil
}

// mapDoneCheck
// Map Task's check after done
// If the task can be found in the c.tasks and the workerId is correct,
// change the tmpFile to the finalFile
func (c *Coordinator) mapDoneCheck(req *Request) {
	lastTaskId := generateTaskId(req.LastTaskType, req.LastTaskIndex)
	c.m.Lock()
	defer c.m.Unlock()
	if task, exist := c.tasks[lastTaskId]; exist && task.WorkerId == req.WorkerId {
		tmpFiles := mapTempFile(task.Index, req.WorkerId, c.nReduce)
		finalFiles := mapFinalFile(task.Index, c.nReduce)
		for i := range tmpFiles {
			e := os.Rename(tmpFiles[i], finalFiles[i])
			if e != nil {
				panic(e)
			}
		}
		delete(c.tasks, lastTaskId)
		if len(c.tasks) == 0 {
			c.forward()
		}
		//fmt.Println("WORKER", req.WorkerId, "finish", lastTaskId)
	} else {
		tmpFiles := mapTempFile(task.Index, req.WorkerId, c.nReduce)
		for _, tmpFile := range tmpFiles {
			os.Remove(tmpFile)
		}
	}
}

// reduceDoneCheck
// Reduce Task's check after done
// If the task can be found in the c.tasks and the workerId is correct,
// change the tmpFile to the finalFile
func (c *Coordinator) reduceDoneCheck(req *Request) {
	lastTaskId := generateTaskId(req.LastTaskType, req.LastTaskIndex)
	c.m.Lock()
	defer c.m.Unlock()
	if task, exist := c.tasks[lastTaskId]; exist && task.WorkerId == req.WorkerId {
		tmpFile := reduceTempFile(task.Index, req.WorkerId)
		finalFile := reduceFinalFile(task.Index)
		e := os.Rename(tmpFile, finalFile)
		if e != nil {
			panic(e)
		}
		delete(c.tasks, lastTaskId)
		if len(c.tasks) == 0 {
			c.forward()
		}
		//fmt.Println("WORKER", req.WorkerId, "finish", lastTaskId)
	} else {
		tmpFile := reduceTempFile(task.Index, req.WorkerId)
		os.Remove(tmpFile)
	}
}

// forward
// Coordinator moves on to the next stage
func (c *Coordinator) forward() {
	if c.stage == "MAP" {
		//fmt.Println("MapReduce now is in REDUCE stage")
		c.stage = "REDUCE"
		reduceTasks := generateReduceTask(c.nReduce)
		for _, reduceTask := range reduceTasks {
			//c.tasks[generateTaskId(reduceTask.Type, reduceTask.Index)] = reduceTask
			c.tasksChan <- reduceTask
		}
	} else if c.stage == "REDUCE" {
		close(c.tasksChan)
		c.stage = "END"
	}
}

// crashMonitor
// Check to see if any tasks are stuck,
// or if there is a Worker downtime that leads to a delay in response.
func (c *Coordinator) crashMonitor() {
	for {
		time.Sleep(500 * time.Millisecond)
		end := func() bool {
			c.m.Lock()
			defer c.m.Unlock()
			if c.stage == "END" {
				return true
			}
			for _, task := range c.tasks {
				if task.WorkerId != -1 && time.Now().After(task.Deadline) {
					task.WorkerId = -1
					c.tasksChan <- task
				}
			}
			return false
		}()
		if end {
			break
		}
	}
}

// server
// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.m.Lock()
	defer c.m.Unlock()
	if c.stage == "END" {
		return true
	}
	return false
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		stage:     "MAP",
		nMap:      len(files),
		nReduce:   nReduce,
		tasks:     make(map[string]Task),
		tasksChan: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	mapTasks := generateMapTask(files)
	for _, mapTask := range mapTasks {
		//c.tasks[generateTaskId(mapTask.Type, mapTask.Index)] = mapTask
		c.tasksChan <- mapTask
	}

	c.server()
	go c.crashMonitor()
	return &c
}
