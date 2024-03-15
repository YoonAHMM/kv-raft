package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

//Get 和 PutAppend都在加入日志，负载较高，优化可以使用readindex
type OpType string
const (
	Opget = "Get"
	Opput = "Put"
	OPappend = "Append"

)

type ReadState struct {
    Index      uint64
    RequestCtx []byte
} 

type Op struct {
	ClientId int
	RequestId uint64

	OpType OpType

	Key string
	Value string

	time int64
}

type OpContent struct{
	Waitch chan string
	Term    int
	index   uint64 //Clientid + Requestid

	Op  *Op
}

type KVServer struct {
	mu      sync.Mutex

	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	commitIndex uint64 
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	KVstore    map[string]string //k-v

	OpMap  map[uint64]*OpContent //index-op
	LastRequestidMap map[int]uint64//ClientId-lastrequest

	LastIncludedIndex int
	// Your definitions here.
}

func NewOpContext(op *Op, term int) *OpContent {
	return &OpContent{
		index : AddIndex(op.ClientId,op.RequestId),
		Op:              op,
		Term:            term,
		Waitch:          make(chan string, 1), //缓冲区1是防止阻塞日志应用协程
	}
}

func AddIndex(clientid int,requestid uint64)uint64{
	return uint64(clientid)+requestid

}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         Opget,
		Key:            args.Key,
		time: time.Now().UnixMilli(),
	}
	
	term := 0
	isLeader := false
	if term, isLeader = kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
	opContext := NewOpContext(op, term)
	kv.OpMap[opContext.index] = opContext
	kv.mu.Unlock()
	_, _, ok := kv.rf.Start(*op)

	defer func() {
		kv.mu.Lock()
		delete(kv.OpMap, opContext.index)
		kv.mu.Unlock()
	}()

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	select {
	case c := <-opContext.Waitch:
		reply.Err = OK
		reply.Value = c
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	
	op := &Op{
		ClientId:       args.ClientId,
		RequestId:      args.RequestId,
		OpType:         OpType(args.Op),
		Key:            args.Key,
		Value:          args.Value,
		time: time.Now().UnixMilli(),
	}
	term:=0
	isLeader:=false
	reply.Err=ErrWrongLeader

	
	kv.mu.Lock()
	if term,isLeader=kv.rf.GetState();!isLeader{
		kv.mu.Unlock()
		return
	}
	
	
	//如果client已经发起过请求，并且已经成功提交，则return
	if lastRequestId, ok := kv.LastRequestidMap[op.ClientId]; ok && lastRequestId >= op.RequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	
	OpContent:=NewOpContext(op,term)

	kv.OpMap[OpContent.index]=OpContent

	kv.mu.Unlock()

	_,_,ok:=kv.rf.Start(*op)
	defer func() {
		kv.mu.Lock()
		delete(kv.OpMap,AddIndex(op.ClientId, op.RequestId))
		kv.mu.Unlock()
	}()
	
	if !ok {
		return
	}

	select {
	case <-OpContent.Waitch:
		reply.Err = OK
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}


// 当不再需要 KVServer 实例时，测试程序员调用 Kill()。
// 为了方便你，我们提供了设置 rf.dead 的代码（无需锁定），
// 以及用于在长时间运行的循环中测试 rf.dead 的 killed() 方法。
// 你也可以在 Kill() 中添加自己的代码。虽然不要求执行任何操作，
// 但这可能会很方便（例如），以阻止从已 Kill() 的实例中输出调试信息。

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}



func (kv *KVServer) applyStateMachineLoop() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					op := applyMsg.Command.(Op)
					
					//保证幂等性
					if op.RequestId <= kv.LastRequestidMap[op.ClientId] {
						return
					}
					if applyMsg.CommandIndex <= kv.LastIncludedIndex && op.OpType != Opget {
						if c, ok := kv.OpMap[AddIndex(op.ClientId, op.RequestId)]; ok {
							c.Waitch <- "0"
						}
						return
					}
					
					switch op.OpType {
					case Opput:
						kv.KVstore[op.Key] = op.Value
						kv.LastRequestidMap[op.ClientId] = op.RequestId
						kv.maybeSnapshot(applyMsg.CommandIndex)
					case OPappend:
						kv.KVstore[op.Key] += op.Value
						kv.LastRequestidMap[op.ClientId] = op.RequestId
						kv.maybeSnapshot(applyMsg.CommandIndex)
					case Opget:
					}
					//使得写入的client能够响应
					val := kv.KVstore[op.Key]
					if content, ok := kv.OpMap[AddIndex(op.ClientId, op.RequestId)]; ok {
						content.Waitch <- val
					}
				}()
			}else{
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					if kv.decodeSnapshot(applyMsg.Snapshot) {
						kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot)
					}
				}()
			}
		}
	}
}


//
// servers[] 包含一组通过 Raft 合作形成容错键值服务的服务器端口。
// me 是当前服务器在 servers[] 中的索引。
// 键值服务器应通过底层 Raft 实现存储快照，
// 该实现应调用 persister.SaveStateAndSnapshot() 来原子地保存 Raft 状态和快照。
// 键值服务器应在 Raft 的保存状态超过 maxraftstate 字节时创建快照，
// 以便允许 Raft 对其日志进行垃圾收集。如果 maxraftstate 为 -1，
// 则无需创建快照。
// StartKVServer() 必须迅速返回，因此应启动用于任何长时间运行工作的 goroutine。
//

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	//调用 labgob.Register 函数，用于你希望 Go 的 RPC 库进行序列化和反序列化的结构体。
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mu=sync.Mutex{}
	// You may need initialization code here.


	kv.KVstore = make(map[string]string)
	kv.OpMap  = make(map[uint64]*OpContent)
	kv.LastRequestidMap = make(map[int]uint64)


	go kv.applyStateMachineLoop()
	return kv
}

func (kv *KVServer) maybeSnapshot(index int){
	if  kv.maxraftstate == -1 {
		return 
	}
	
	if kv.rf.Loglen() > kv.maxraftstate{
		kv.rf.Snapshot(index,kv.encodeSnapshot(index))
	}
}

func (kv *KVServer) encodeSnapshot(Index int) []byte {
	
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.KVstore)
	e.Encode(Index)
	e.Encode(kv.LastRequestidMap)

	return w.Bytes()
}


func (kv *KVServer) decodeSnapshot(snapshot []byte)bool{
	if len(snapshot) == 0{
		return  true
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.KVstore); err != nil {
		return false
	}
	if err := d.Decode(&kv.LastRequestidMap); err != nil {
		return false
	} 
	if err := d.Decode(&kv.LastIncludedIndex);err !=nil{
		return false
	}

	return true
}