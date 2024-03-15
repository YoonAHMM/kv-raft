package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"sort"
	"time"

	"encoding/json"
	"os"
	"path/filepath"
)

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

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		for {
			task:=GetTask()
			switch task.Taskstage {
				case Exit:
					return
				case Map:
					Maper(&task,mapf)
				case Reduce:
					Reducer(&task,reducef)
				case Wait:
					time.Sleep(1*time.Second)
			}
		}
}

func GetTask() Task{
	arg:=ExampleArgs{}
	task:=Task{}

	call("Master.WorkerCall",&arg,&task)
	return task
}

func TaskUpdata(task Task){
	reply:=ExampleReply{}

	call("Master.TaskUpdata",task,&reply)
}

func Maper(task *Task,mapf func(string, string) []KeyValue){
	file,err:=ioutil.ReadFile(task.Input)
	if err!=nil{
		log.Fatal("Failed to open file"+task.Input,err)
	}

	intermediates:=mapf(task.Input,string(file))

	kvs:=make([][]KeyValue,task.NReduce)

	for _, intermediate := range intermediates {
		k := ihash(intermediate.Key) % task.NReduce
		kvs[k] = append(kvs[k], intermediate)
	}

	mapoutput:=make([]string,0)
	for i:=0;i<task.NReduce;i++{
		mapoutput=append(mapoutput,writeToLocalFile(task.TaskId, i, &kvs[i]))
	}
	task.Intermediates=mapoutput

	TaskUpdata(*task)

}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	enc := json.NewEncoder(tempFile)

	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

//
// call Reduce on each distinct key in intermediate[],
// and print the result to mr-out-0.
//
func Reducer(task *Task,reducef func(string, []string) string){
	intermediate := ReadReduceFile(task.Intermediates)

	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		//统计相同单词的数量
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)//len(values)==单词数量

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-out-%d",task.TaskId)
	os.Rename(tempFile.Name(), outputName)
	task.Output=outputName
	TaskUpdata(*task)
}


func ReadReduceFile(filenames []string)[]KeyValue{
	kvs:=[]KeyValue{}

	for _,filename:=range filenames{
		file,err:=os.Open(filename)
		if err!=nil{
			log.Fatal("Failed to open file "+filename, err)
		}
		filedec:=json.NewDecoder(file)

		for {
			var kv KeyValue
			if err:=filedec.Decode(&kv);err!=nil{
				break
			}
			kvs=append(kvs,kv)
		}
		file.Close()
	}
	return kvs
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

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
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
