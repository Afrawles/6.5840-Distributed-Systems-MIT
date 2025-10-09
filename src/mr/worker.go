package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, reply *TaskResponse) {
	mapIndex, err := strconv.Atoi(strings.TrimPrefix(reply.TaskID, "m-"))
	if err != nil {
		log.Fatalf("Invalid map TaskID: %v", reply.TaskID)
	}

	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File)
	}
	file.Close()
	kva := mapf(reply.File, string(content))
	buckets := make([][]KeyValue, reply.NReduce)

	// results mapped into Nreduce buckets 
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		buckets[r] = append(buckets[r], kv)
	}

	for r := 0; r < reply.NReduce; r++ {
		ifilename := fmt.Sprintf("mr-%d-%d", mapIndex, r)

		fmt.Fprintf(os.Stderr, "[Worker]: Writing to %v\n", ifilename)

		file, err := os.Create(ifilename)
		if err != nil {
			log.Fatalf("failed to create %v", ifilename)
		}

		enc := json.NewEncoder(file)
		for _, kv := range buckets[r] {
			enc.Encode(kv)
		}

		file.Close()
	}
}


func doReduceTask(reducef func(string, []string) string, reply *TaskResponse) {
	var intermediate []KeyValue

	for m:=0; m < reply.NMap; m++ {
		ifile := fmt.Sprintf("mr-%d-%d", m, reply.ReduceIndex)
		file, err := os.Open(ifile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Worker]: Cannot open %v: %v\n", ifile, err)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	ofilename := fmt.Sprintf("mr-out-%d", reply.ReduceIndex)
	
	fmt.Fprintf(os.Stderr, "[Worker]: Writing to %v\n", ofilename)

	ofile, err := os.Create(ofilename)
	if err != nil {
		log.Fatalf("cannot create %v", ofilename)
	}

	defer ofile.Close()

	i := 0 
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	
	for {
		reply := &TaskResponse{}
		if !call("Coordinator.HandleTaskRequest", &TaskRequest{}, reply) {
			os.Exit(0)
		}

		if reply.TaskID == "" {
			fmt.Fprintf(os.Stderr, "[Worker]: No task assigned, retrying\n")
			time.Sleep(time.Second)
			continue 
		}

		fmt.Fprintf(os.Stderr, "[Worker]: Processing task %v (type %v)\n", reply.TaskID, reply.TaskType)

		switch reply.TaskType {
		case TaskTypeMap:
			fmt.Fprintf(os.Stderr, "[Worker]: Starting map task %v\n", reply.TaskID)
			doMapTask(mapf, reply)
			fmt.Fprintf(os.Stderr, "[Worker]: Completed map task %v\n", reply.TaskID)
		case TaskTypeReduce:
			fmt.Fprintf(os.Stderr, "[Worker]: Starting reduce task %v\n", reply.TaskID)
			doReduceTask(reducef, reply)
		fmt.Fprintf(os.Stderr, "[Worker]: Completed reduce task %v\n", reply.TaskID)
		} 

		doneReq := TaskDoneRequest{TaskID: reply.TaskID, TaskType: reply.TaskType}
		doneResp := TaskDoneResponse{}

		fmt.Fprintf(os.Stderr, "[Worker]: Reporting task %v done\n", reply.TaskID)

		if call("Coordinator.HandleTaskDone", &doneReq, &doneResp) {
			fmt.Fprintf(os.Stderr, "[Worker]: Successfully reported task %v done\n", reply.TaskID)
		} else {
			fmt.Fprintf(os.Stderr, "[Worker]: Failed to report task %v done\n", reply.TaskID)
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
