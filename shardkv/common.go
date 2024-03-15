package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientID int64
	RequestID int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientID int64
	RequestID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Operation string
const (
	Put         Operation = "Put"
	Append                = "Append"
	Get                   = "Get"
	Config                = "Config"
	AddShard              = "AddShard"
	RemoveShard           = "RemoveShard"
)