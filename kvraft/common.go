package kvraft

import "log"
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string


// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string 
	ClientId int
	RequestId uint64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int
	RequestId uint64
}

type GetReply struct {
	Err   Err
	Value string
}


const Debug = false
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
