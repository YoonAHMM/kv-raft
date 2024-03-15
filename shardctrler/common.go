package shardctrler


//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.

//互不相交并且组成完整数据的每一个数据子集称为 Shard
//在同一阶段中，Shard 与集群的对应关系称为 Config
//
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrTimeout     = "ErrTimeout"
)
type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientID int64
	RequestID int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientID int64
	RequestID int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientID int64
	RequestID int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientID int64
	RequestID int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type OpType string
const (
	OpJoin = "Join"
	OpLeave = "Leave"
	OPMove = "Move"
	OPQuery = "Query"

)
