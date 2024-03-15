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

var mu sync.Mutex 

type MasterTaskStores int
const (
	Idel MasterTaskStores=iota
	Inprogres
	Completed 
)

type Stage int
const (
	Map Stage=iota
	Reduce
	Wait
	Exit
)




type Master struct {
	WaitingTask chan *Task //所有等待执行的task
	TaskStatusMap map[int]*Task//master储存task的所有任务状态
	
	WorkerId map[string]int

	MasterStage Stage

	NMap int
	NRedue int

	InputFile []string
	Intermediates [][]string
}


type Task struct{
	TaskStores  MasterTaskStores
	StartTime time.Time
	TaskId int
	Taskstage Stage
	Input string //map输入文件名
	Intermediates []string //reduce输入文件名数组
	Output string
	NReduce int
}


//
// start a thread that listens for RPCs from worker.go
//
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
//main/mrmaster.go 定期调用 Done() 函数以查看整个作业是否已经完成。

//直接获取master在哪个阶段
func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret:= m.MasterStage==Exit
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func max(a,b int)int{
	if a>b{
		return a
	}
	return b
}
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		WaitingTask: make(chan *Task,max(len(files),nReduce)),
		NMap: len(files),
		NRedue: nReduce,
		TaskStatusMap:make(map[int]*Task),
		WorkerId:make(map[string]int),
		MasterStage:Map,
		InputFile:files,
		Intermediates  :make([][]string,nReduce),
	}

	m.MapTask()

	m.server()

	go m.TaskTimeoutMonitor()

	return &m
}


func(m *Master)MapTask(){
	for idx,filename:=range m.InputFile{
		task:=Task{
			TaskId :idx,
			Input :filename,
			Taskstage: Map,
			NReduce:m.NRedue,
		}
		m.WaitingTask<- &task
		m.TaskStatusMap[idx]=&task
	}
}

func(m *Master)ReduceTask(){
	m.TaskStatusMap=make(map[int]*Task)
	for idx,filename:=range m.Intermediates {
		task:=Task{
			TaskId :idx,
			Intermediates :filename,
			Taskstage: Reduce,
			NReduce:m.NRedue,
		}

		m.WaitingTask<- &task
		m.TaskStatusMap[idx]=&task
	}
}

func(m *Master)WorkerCall(arg *ExampleArgs,task *Task)error{
	mu.Lock()
	defer mu.Unlock()

	if len(m.WaitingTask)>0{
		*task = *<- m.WaitingTask
		m.TaskStatusMap[task.TaskId].StartTime=time.Now()
		m.TaskStatusMap[task.TaskId].TaskStores=Inprogres
		
	}else if m.MasterStage == Exit{
		*task=Task{Taskstage: Exit}
	}else{
		*task=Task{Taskstage: Wait}
	}
	return nil
}

func(m *Master)TaskUpdata(task *Task,reply *ExampleReply)error{
	mu.Lock()
	defer mu.Unlock()

	//当前阶段不同，或master中已记录这个task状态为已完成情况跳过此次task
	if m.MasterStage!=task.Taskstage||m.TaskStatusMap[task.TaskId].TaskStores==Completed{
		return nil
	}else{
		m.TaskStatusMap[task.TaskId].TaskStores=Completed
		go m.WriteDisk(task)
	}
	return nil
}
func (m *Master)WriteDisk(task *Task){
	mu.Lock()
	defer mu.Unlock()

	switch m.MasterStage{
		case Map:
			for idx,filename:=range task.Intermediates{
				m.Intermediates[idx]=append(m.Intermediates[idx], filename)
			}
			if m.AllTackDone(){
				m.ReduceTask()
				m.MasterStage=Reduce
			}
		case Reduce:
			if m.AllTackDone(){
				m.MasterStage=Exit
			}
	}
}

func (m *Master)AllTackDone()bool{
	for _,tack:=range m.TaskStatusMap{
		if tack.TaskStores!=Completed{
			return false
		}
	}
	return true
}

func (m *Master)TaskTimeoutMonitor(){
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.MasterStage == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskStatusMap {
			if masterTask.TaskStores == Inprogres && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				m.WaitingTask <- masterTask
				masterTask.TaskStores = Idel
			}
		}
		mu.Unlock()
	}
}