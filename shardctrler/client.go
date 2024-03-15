package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	
	mu sync.Mutex

	clientid int64//clientid

	lastServerId int

	requestId int64

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
	ck.clientid=nrand()
	return ck
}



func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.ClientID=ck.clientid
	args.RequestID = atomic.AddInt64(&ck.requestId,1)

	serverid := ck.lastServerId
	for {
		var reply QueryReply
		ok := ck.servers[serverid].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastServerId=serverid
			return reply.Config
		}
		serverid ++
		serverid %=len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}


func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID=ck.clientid
	args.RequestID = atomic.AddInt64(&ck.requestId,1)

	serverid := ck.lastServerId
	for {
		var reply QueryReply
		ok := ck.servers[serverid].Call("ShardCtrler.join", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastServerId=serverid
			return
		}
		serverid ++
		serverid %=len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	args.ClientID=ck.clientid
	args.RequestID = atomic.AddInt64(&ck.requestId,1)

	serverid := ck.lastServerId
	for {
		var reply LeaveReply
		ok := ck.servers[serverid].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastServerId=serverid
			return
		}
		serverid ++
		serverid %=len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.ClientID=ck.clientid
	args.RequestID = atomic.AddInt64(&ck.requestId,1)

	serverid := ck.lastServerId
	for {
		var reply MoveReply
		ok := ck.servers[serverid].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastServerId=serverid
			return
		}
		serverid ++
		serverid %=len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
