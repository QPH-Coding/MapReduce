package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ihash
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// CallExample()
	pid := os.Getpid()
	req := Request{
		WorkerId:      pid,
		LastTaskType:  "",
		LastTaskIndex: 0,
	}
ReqLoop:
	for {
		// The statement of the `resp` must be here
		// Or it will not get the REDUCE-0,
		// because 0 is the Zero-Value of the `resp.Index`
		var resp Response
		ok := call("Coordinator.RequestTask", &req, &resp)
		if !ok {
			//log.Fatalf("Worker %d can not call RPC", pid)
			break ReqLoop
		}
		//fmt.Println("--------\nWORKER", pid, "receive TASK:\n", resp)
		switch resp.Task.Type {
		case "MAP":
			mapWork(&req, &resp, mapf)
		case "REDUCE":
			reduceWork(&req, &resp, reducef)
		}
	}
}

// mapWork
// Worker do map work
func mapWork(req *Request, resp *Response, mapf func(string, string) []KeyValue) {
	if resp.Task.WorkerId != req.WorkerId {
		req.LastTaskType = ""
	} else {
		req.LastTaskType = resp.Task.Type
		req.LastTaskIndex = resp.Task.Index
		file, err := os.Open(resp.Task.MapInputFile)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		content, err := io.ReadAll(file)
		if err != nil {
			panic(err)
		}
		kva := mapf(resp.Task.MapInputFile, string(content))
		writeTempIntermediate(resp, &kva)
	}
}

// writeTempIntermediate
// Worker storage the temp intermediate files as Json after doing map task
func writeTempIntermediate(resp *Response, kva *[]KeyValue) {
	nReduce := resp.ReduceNum
	tmpFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	tmpFileNames := mapTempFile(resp.Task.Index, resp.Task.WorkerId, nReduce)
	for i, tmpFileName := range tmpFileNames {
		var err error
		tmpFiles[i], err = os.Create(tmpFileName)
		if err != nil {
			panic(err)
		}
		defer tmpFiles[i].Close()
		encoders[i] = json.NewEncoder(tmpFiles[i])
	}
	for _, kv := range *kva {
		index := ihash(kv.Key) % nReduce
		encoders[index].Encode(&kv)
	}
}

// reduceWork
// Worker do reduce task
func reduceWork(req *Request, resp *Response, reducef func(string, []string) string) {
	if resp.Task.WorkerId != req.WorkerId {
		req.LastTaskType = ""
		fmt.Println("Task", generateTaskId(resp.Task.Type, resp.Task.Index), "should be apply to",
			resp.Task.WorkerId, "not WORKER", req.WorkerId)
	} else {
		req.LastTaskType = resp.Task.Type
		req.LastTaskIndex = resp.Task.Index
		var kva []KeyValue
		kvm := readIntermediate(resp)
		for k, v := range *kvm {
			kv := KeyValue{k, reducef(k, v)}
			kva = append(kva, kv)
		}
		tmpFileName := reduceTempFile(resp.Task.Index, req.WorkerId)
		tmpFile, err := os.Create(tmpFileName)
		if err != nil {
			panic(err)
		}
		defer tmpFile.Close()
		for _, kv := range kva {
			fmt.Fprintf(tmpFile, "%v %v\n", kv.Key, kv.Value)
		}
	}
}

// readIntermediate
// Worker read intermediate file like "mr-X-Y" before doing reduce task
func readIntermediate(resp *Response) *map[string][]string {
	kvm := map[string][]string{}
	readFileNames := reduceReadFile(resp.Task.Index, resp.MapNum)
	for _, readFileName := range readFileNames {
		func(readFileName string) {
			readFile, err := os.Open(readFileName)
			if err != nil {
				panic(err)
			}
			defer readFile.Close()
			dec := json.NewDecoder(readFile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err == nil {
					kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
				} else {
					break
				}
			}
		}(readFileName)
	}
	return &kvm
}

// call
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
