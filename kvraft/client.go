package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)


var clientID int32

type Clerk struct {
	mu  sync.Mutex
	servers []*labrpc.ClientEnd

	id int//clientid

	lastServerId int

	requestId uint64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.mu = sync.Mutex{}
	ck.lastServerId = 0;
	ck.requestId=1;
	ck.id = int(atomic.AddInt32(&clientID,1))
	// You'll have to add code here.
	return ck
}


// 获取指定键的当前值。
// 如果键不存在，返回空字符串。
// 在面对其他所有错误时，将永远不停地尝试。
//
// 您可以使用以下代码发送RPC：
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// args和reply的类型（包括它们是否为指针）
// 必须与RPC处理程序函数参数的声明类型匹配。并且reply必须以指针形式传递。
//


func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	defer func(){
		DPrintf("client %d , requestid %d : Get(), Key : %v.........success",ck.id,ck.requestId,key)
	}()
	args:=&GetArgs{
		Key: key,
		ClientId: ck.id,
		RequestId: ck.requestId,
	}

	ck.requestId++
	RPCId:=ck.lastServerId


	for {
		reply:=&GetReply{}
		ok:=ck.servers[RPCId].Call("KVServer.Get",args,reply)
		
		if !ok{
			RPCId++
			RPCId %= len(ck.servers)
		}else{
			switch reply.Err{
			case "OK":
				ck.lastServerId=RPCId
				ck.lastServerId %=len(ck.servers)
				return reply.Value
			case"ErrNoKey":
				ck.lastServerId=RPCId
				ck.lastServerId %=len(ck.servers)
				return ""
			case"ErrWrongLeader":
				RPCId++;
				RPCId %= len(ck.servers)
			case "ErrTimeout":
			}
		}
		time.Sleep(time.Millisecond * 1)
	}
}


// 由Put和Append共享。
//
// 您可以使用以下代码发送RPC：
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// args和reply的类型（包括它们是否为指针）
// 必须与RPC处理程序函数参数的声明类型匹配。并且reply必须以指针形式传递。
//

func (ck *Clerk) PutAppend(key string, value string, op string){
	
	ck.mu.Lock()
	defer ck.mu.Unlock()

	defer func(){
		DPrintf("client %d , requestid %d : PutAppened() , Key : %v value : %v , op : %v .........success",ck.id,ck.requestId,key,value,op)
	}()

	args:=&PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId:ck.id,
		RequestId: ck.requestId,
	}
	ck.requestId++
	RPCId:=ck.lastServerId
	
	for {
		reply:=&PutAppendReply{}
		ok:=ck.servers[RPCId].Call("KVServer.PutAppend",args,reply)
		
		if !ok{
			RPCId++
			RPCId %= len(ck.servers)
		}else{
			switch reply.Err{
			case "OK":
				ck.lastServerId=RPCId
				ck.lastServerId %=len(ck.servers)
				return
			case"ErrWrongLeader":
				RPCId++;
				RPCId %= len(ck.servers)
			case "ErrTimeout":
			}
	
		}
		time.Sleep(time.Millisecond * 1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Put   %s   %s",key,value)
	ck.PutAppend(key, value, "Put")
	
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("Put   %s   %s",key,value)
	ck.PutAppend(key, value, "Append")
	
}
