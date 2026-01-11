package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		taskreply := AssignTaskReply{}
		taskargs := AssignTaskArgs{}
		ok := call("Master.AssignTask", &taskargs, &taskreply)

		if !ok {
			log.Printf("AssignTask Failed, Sleep 1s then retry..")
			time.Sleep(time.Second)
			continue
		}

		switch taskreply.Tasktype {
		case MapTask:
			doMap(mapf, taskreply.Taskid, taskreply.Filename, taskreply.NReduce)
			doneargs := ReportTaskDoneArgs{Tasktype: MapTask, Taskid: taskreply.Taskid}
			donereply := ReportTaskDoneReply{}
			mapreport := call("Master.ReportTaskDone", &doneargs, &donereply)
			
			if !mapreport {
				log.Printf("MapTask ReportTaskDone Failed, Sleep 1s")
				time.Sleep(time.Second)
				continue
			}

		case ReduceTask:
			doReduce(reducef, taskreply.Taskid, taskreply.NMap)
			doneargs := ReportTaskDoneArgs{Tasktype: ReduceTask, Taskid: taskreply.Taskid}
			donereply := ReportTaskDoneReply{}
			reducereport := call("Master.ReportTaskDone", &doneargs, &donereply)

			if !reducereport {
				log.Printf("ReduceTask ReportTaskDone Failed, Sleep 1s")
				time.Sleep(time.Second)
				continue
			}
		case Wait:
			time.Sleep(time.Second)

		case AllDone:
			return 
		}
	}
	
	// call("Master.Done", nil, nil)
}

func doMap(mapf func(string, string) []KeyValue, taskid int, filename string, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucket_id := ihash(kv.Key) % nReduce // 同一 key 总是落在同一个 Reduce 任务
		buckets[bucket_id] = append(buckets[bucket_id], kv)
	}

	for reduceId, kvList := range buckets {
		jsonfileName := fmt.Sprintf("mr-%d-%d", taskid, reduceId)
		file_json, err := os.Create(jsonfileName)

		if err != nil {
			log.Fatal("Cannot create %v", jsonfileName)
		}

		enc := json.NewEncoder(file_json)
		for _, kv := range kvList {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("encode error %v", kv)
			}
		}

		file_json.Close()
	}
}

func doReduce(reducef func(string, []string) string, taskid int, nMap int) {
	kva := []KeyValue{}
	for mapId := 0; mapId < nMap; mapId ++ {
		filename := fmt.Sprintf("mr-%d-%d", mapId, taskid)
		file, err := os.Open(filename)
		if err != nil {
			continue 
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", taskid)
	ofile, err := os.Create(oname)

	if err != nil {
    	log.Fatalf("cannot create %v", oname)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
