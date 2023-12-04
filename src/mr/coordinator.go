package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files  []string
	status int //1:map 阶段 2:reduce 阶段

	numMapTotal int
	//已分配的map任务数量,从0开始，下标是最新一个未分配的任务
	numMapCreated int
	numMapDone    int
	mapTasks      []MapTask

	nReduce          int
	numReduceCreated int
	numReduceDone    int
	reduceTasks      []ReduceTask
}

// 定义一个reduce task结构
type ReduceTask struct {
	TaskID    int
	status    int //0:未分配 1:已分配 2:已完成
	workerId  int //分配的worker id
	timebegin int //开始时间
}

// 定义一个MAP task结构
type MapTask struct {
	TaskID    int
	fileName  string
	status    int //0:未分配 1:已分配 2:已完成
	workerId  int //分配的worker id
	timebegin int //开始时间
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MRCall(args *MRCallArgs, reply *MRCallReply) (err error) {
	//统一设置的部分
	reply.NReduce = c.nReduce

	if args.Status == 0 {
		//空闲
		if c.status == 1 {
			//map阶段
			err = c.MapCall(args, reply)
		} else if c.status == 2 {
			//reduce阶段
			err = c.ReduceCall(args, reply)
		}
	} else {
		//完成任务
		if c.status == 1 {
			//map阶段
			err = c.MapCommit(args, reply)
			if err != nil {
				return err
			}
			err = c.MapCall(args, reply)
		} else if c.status == 2 {
			//reduce阶段
			err = c.ReduceCommit(args, reply)
			if err != nil {
				return err
			}
			err = c.ReduceCall(args, reply)
		}
	}
	return err
}

func (c *Coordinator) MapCall(args *MRCallArgs, reply *MRCallReply) error {
	//获取系统时间s
	s := time.Now().Unix()
	//优先分配未分配的任务
	if c.numMapCreated < c.numMapTotal {
		c.mapTasks[c.numMapCreated].status = 1
		c.mapTasks[c.numMapCreated].workerId = args.WorkerID
		c.mapTasks[c.numMapCreated].timebegin = int(s)
		reply.TaskType = 1
		reply.TaskID = c.numMapCreated
		reply.MapFileName = c.mapTasks[c.numMapCreated].fileName
		c.numMapCreated++
		return nil
	}
	//需要寻找失效的任务
	//todo：

	//让worker等待
	reply.TaskType = 3

	return nil
}
func (c *Coordinator) ReduceCall(args *MRCallArgs, reply *MRCallReply) error {
	//获取系统时间s
	s := time.Now().Unix()
	if c.numReduceCreated < c.nReduce {
		c.reduceTasks[c.numReduceCreated].status = 1
		c.reduceTasks[c.numReduceCreated].workerId = args.WorkerID
		c.reduceTasks[c.numReduceCreated].timebegin = int(s)
		reply.TaskType = 2
		reply.TaskID = c.numReduceCreated
		c.numReduceCreated++
		return nil
	}
	//需要寻找失效的任务
	//todo：

	//让worker等待
	reply.TaskType = 3
	return nil
}
func (c *Coordinator) MapCommit(args *MRCallArgs, reply *MRCallReply) error {
	taskid := args.TaskID
	c.mapTasks[taskid].status = 2
	c.numMapDone++
	if c.numMapDone == c.numMapTotal {
		//map阶段完成
		c.status = 1
	}
	return nil
}
func (c *Coordinator) ReduceCommit(args *MRCallArgs, reply *MRCallReply) error {
	taskid := args.TaskID
	c.reduceTasks[taskid].status = 2
	c.numReduceDone++
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.

	return c.numReduceDone == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	//获取files长度
	mapsLen := len(files)
	c.numMapTotal = mapsLen

	c.numMapCreated = 0
	c.numMapDone = 0
	c.numReduceDone = 0
	c.numReduceCreated = 0

	//初始化mapTasks
	c.mapTasks = make([]MapTask, mapsLen)
	for i := 0; i < mapsLen; i++ {
		c.mapTasks[i].TaskID = i
		c.mapTasks[i].fileName = files[i]
		c.mapTasks[i].status = 0
		c.mapTasks[i].workerId = -1
		c.mapTasks[i].timebegin = 0
	}
	//初始化reduceTasks
	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i].TaskID = i
		c.reduceTasks[i].status = 0
		c.reduceTasks[i].workerId = -1
		c.reduceTasks[i].timebegin = 0
	}
	//开始map阶段
	c.status = 1

	c.server()
	return &c
}
