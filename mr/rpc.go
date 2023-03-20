package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Task struct {
	Type         string //MAP, REDUCE, WAIT, END
	Index        int
	WorkerId     int
	MapInputFile string
	Deadline     time.Time
}

type Request struct {
	WorkerId      int
	LastTaskType  string
	LastTaskIndex int
}

type Response struct {
	Task      Task
	MapNum    int
	ReduceNum int
}

func generateMapTask(files []string) []Task {
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i] = Task{
			Type:         "MAP",
			Index:        i,
			MapInputFile: file,
			WorkerId:     -1,
		}
	}
	return tasks
}

func generateReduceTask(nReduce int) []Task {
	tasks := make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		tasks[i] = Task{
			Type:     "REDUCE",
			Index:    i,
			WorkerId: -1,
		}
	}
	return tasks
}

func mapTempFile(index, workerId, nReduce int) []string {
	dir, _ := os.Getwd()
	tmpFiles := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		tmpFiles[i] = dir + "/mr-m-tmp-" + strconv.Itoa(index) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(workerId)
	}
	return tmpFiles
}

func mapFinalFile(index, nReduce int) []string {
	dir, _ := os.Getwd()
	finalFiles := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		finalFiles[i] = dir + "/mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(i)
	}
	return finalFiles
}

func reduceTempFile(index, workerId int) string {
	dir, _ := os.Getwd()
	tmpFile := dir + "/mr-r-tmp-" + strconv.Itoa(index) + "-" + strconv.Itoa(workerId)
	return tmpFile
}

func reduceFinalFile(index int) string {
	dir, _ := os.Getwd()
	finalFile := dir + "/mr-out-" + strconv.Itoa(index)
	return finalFile
}

func reduceReadFile(index, nMap int) []string {
	dir, _ := os.Getwd()
	readFiles := make([]string, nMap)
	for i := 0; i < nMap; i++ {
		readFiles[i] = dir + "/mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(index)
	}
	return readFiles
}

func generateTaskId(taskType string, index int) string {
	return fmt.Sprintf("%s-%d", taskType, index)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
