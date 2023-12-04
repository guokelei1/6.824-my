package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

var GlobalWorkerID int
var NReduce int
var NMap int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	GlobalWorkerID = os.Getpid()
	ok := CallRegiste(GlobalWorkerID)
	if !ok {
		return
	}
	DPrintf("Worker get NReduce:%+v", NReduce)
	// Your worker implementation here.

	//不断循环处理
	for {
		args := GetOneTaskArgs{WorkerID: GlobalWorkerID}
		reply := GetOneTaskReply{}
		ok := CallGetOneTask(&args, &reply)
		if !ok {
			time.Sleep(1 * time.Second)
			continue
		}
		//根绝TaskType进行不同处理
		if reply.TaskType == 3 {
			//wait
			time.Sleep(5 * time.Second)
			continue
		} else if reply.TaskType == 1 {
			//处理map相关逻辑
			ok := handleMapTask(reply.TaskID, reply.MapFileName, mapf)
			if !ok {
				CallCommitTask(reply.TaskType, reply.TaskID, 1)
			} else {
				//提交任务
				CallCommitTask(reply.TaskType, reply.TaskID, 0)
			}
		} else if reply.TaskType == 2 {
			//处理reduce相关逻辑
			ok := handleReduceTask(reply.TaskID, reducef)
			if !ok {
				CallCommitTask(reply.TaskType, reply.TaskID, 0)
			} else {
				//提交任务
				CallCommitTask(reply.TaskType, reply.TaskID, 1)
			}
		} else if reply.TaskType == 4 {
			//结束程序
			break
		}

	}

}

func handleMapTask(taskid int, filename string, mapf func(string, string) []KeyValue) bool {
	file, err := os.Open(filename)
	if err != nil {
		DPrintf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		DPrintf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//将kva写入中间文件
}

func handleReduceTask(taskid int, reducef func(string, []string) string) bool {
	//读取中间文件
	//根据key进行排序
	//将排序后的结果写入文件
	return false
}

// 注册WorkID 以及设置NReduce
func CallRegiste(workerid int) bool {
	args := &RegisteArgs{}
	args.WorkerID = int(workerid)
	reply := &RegisteReply{}
	ok := call("Coordinator.Registe", &args, &reply)
	if !ok {
		DPrintf("call failed!\n")
	} else {
		NReduce = reply.NReduce
		NMap = reply.NMap
	}
	return ok
}

func CallGetOneTask(args *GetOneTaskArgs, reply *GetOneTaskReply) bool {
	return call("Coordinator.GetOneTask", args, reply)
}
func CallCommitTask(tasktype int, taskid int, status int) bool {
	args := CommitTaskArgs{
		WorkerID: GlobalWorkerID,
		TaskID:   taskid,
		TaskType: tasktype,
		Status:   status,
	}
	reply := CommitTaskReply{}

	ok := call("Coordinator.Registe", &args, &reply)
	if !ok {
		DPrintf("call failed!\n")
	}
	return ok
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
