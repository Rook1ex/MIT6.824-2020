package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 任务状态
type TaskState int
const (
	Idle TaskState = 0
	InProgress TaskState = 1
	Done TaskState = 2
	Timeout TaskState = 3
)

// 任务类型
type TaskType int
const (
    MapTask TaskType = iota
    ReduceTask
    Wait
    AllDone
)

const TaskTimeout = 10 * time.Second

// MapReduce 整个系统所处的阶段
type Phase int
const (
	MapPhase Phase = 0
	ReducePhase Phase = 1
	DonePhase Phase = 2
)

// Task结构体表示状态和开始时间
type Task struct {
	state TaskState
	startTime time.Time  
}


type Master struct {
	// Your definitions here.
	mu sync.Mutex

	mapTasks []Task // store all map tasks 
	reduceTasks []Task // store all reduce tasks 

	files []string // 
	nReduce int // number of reduce thread
	nMap int // number of map thread

	phase Phase // current phase of whole mapreduce 
}

// Your code here -- RPC handlers for the worker to call.

func (master *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	master.mu.Lock()
	defer master.mu.Unlock()

	if master.phase == MapPhase {
		len := len(master.mapTasks)
		sum := 0  // 计算完成maptask 数量
		// find idle task, send the task to worker for completing
		for i := 0; i < len; i ++ {
			if master.mapTasks[i].state == Idle {
				master.mapTasks[i].state = InProgress
				master.mapTasks[i].startTime = time.Now()

				reply.Tasktype = MapTask
				reply.Taskid = i
				reply.Filename = master.files[i]
				reply.NReduce = master.nReduce

				return nil
			} else if master.mapTasks[i].state == Done {
				sum += 1
			}
		}

		if sum == len {
			// reply.tasktype = AllDone
			master.phase = ReducePhase
		}

		reply.Tasktype = Wait
		return nil
	} else if (master.phase == ReducePhase) {
		len := len(master.reduceTasks)
		sum := 0

		for i := 0; i < len; i ++ {
			if master.reduceTasks[i].state == Idle {
				master.reduceTasks[i].state = InProgress
				master.reduceTasks[i].startTime = time.Now()

				reply.Tasktype = ReduceTask
				reply.Taskid = i
				// reply.nMap = master.nReduce
				reply.NMap = master.nMap

				return nil
			} else if master.reduceTasks[i].state == Done {
				sum += 1
			}
		}

		if sum == len {
			master.phase = DonePhase
			reply.Tasktype = AllDone
			return nil
		}
		
		reply.Tasktype = Wait
		return nil

	} else if master.phase == DonePhase {
		reply.Tasktype = AllDone
		return nil
	} else {
		reply.Tasktype = AllDone
		return nil
	}
}

func (master *Master) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	master.mu.Lock()
	defer master.mu.Unlock()

	if args.Tasktype == MapTask {
		master.mapTasks[args.Taskid].state = Done
	} else if args.Tasktype == ReduceTask {
		master.reduceTasks[args.Taskid].state = Done
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//

func (m *Master) checkTimeoutTask() {
	for {
		time.Sleep(time.Second)
		m.mu.Lock()

		if m.phase == DonePhase {
			m.mu.Unlock()
			break
		}

		for i := range m.mapTasks {
			t := &m.mapTasks[i]
			if t.state == InProgress && time.Since(t.startTime) > TaskTimeout {
				t.state = Idle
			}
		}

		for i := range m.reduceTasks {
			t := &m.reduceTasks[i]
			if t.state == InProgress && time.Since(t.startTime) > TaskTimeout {
				t.state = Idle
			}
		}
		m.mu.Unlock()
	}
}

func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := false

	// Your code here.
	if m.phase == DonePhase {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.nReduce = nReduce
	m.phase = MapPhase
	m.nMap = len(files)

	m.mapTasks = make([]Task, m.nMap)
	for i := range m.mapTasks {
		m.mapTasks[i].state = Idle
	}

	m.reduceTasks = make([]Task, m.nReduce)
	for i := range m.reduceTasks {
		m.reduceTasks[i].state = Idle
	}

	m.server()
	go m.checkTimeoutTask()
	return &m
}
