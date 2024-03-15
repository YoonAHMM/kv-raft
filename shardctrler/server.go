package shardctrler


import (
	"6.824/raft"
	"6.824/labrpc"
	"sync"
	"6.824/labgob"
	"github.com/mitchellh/mapstructure"
	"sort"
	"time"
)
type ShardCtrler struct {
	mu      sync.Mutex
	me      int

	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	
	version int

	configs []Config // indexed by config num

	OpMap  map[uint64]*OpContent //index-op
	LastRequestidMap map[int]int64//ClientId-lastrequest

}


type Op struct {
	ClientId int64
	RequestId int64

	Version int
	OpType OpType
	Time int64

	Shard int
	Gid int

	Groups map[int][]string
	Gids []int //LeaveGids
}

type OpContent struct{
  	Op     *Op
	Waitch chan Config
	Term    int
	index   uint64 //Clientid + Requestid
  
}
func NewOpContext(op *Op, term int) *OpContent {
	return &OpContent{
		index : AddIndex(int(op.ClientId),uint64(op.RequestId)),
		Op:              op,
		Term:            term,
		Waitch:          make(chan Config, 1), //缓冲区1是防止阻塞日志应用协程
	}
}
func AddIndex(clientid int,requestid uint64)uint64{
	return uint64(clientid)+requestid
}

func GroupCopy(src map[int][]string ) map[int][]string {
	dst := make(map[int][]string)
	err := mapstructure.Decode(src, &dst)
	if err != nil {
	  panic(err)
	}
	return dst
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	
	op := &Op{
		ClientId:       args.ClientID,
		RequestId:      args.RequestID,
		OpType:         OpType("Join"),
		Time:           time.Now().UnixMilli(),
	}
	term:=0
	isLeader:=false
	reply.WrongLeader=false

	
	sc.mu.Lock()
	if term,isLeader=sc.rf.GetState();!isLeader{
		sc.mu.Unlock()
		return
	}
	
	
	//如果client已经发起过请求，并且已经成功提交，则return
	if lastRequestId, ok := sc.LastRequestidMap[int(op.ClientId)]; ok && lastRequestId >= op.RequestId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	
	OpContent:=NewOpContext(op,term)

	sc.OpMap[OpContent.index]=OpContent

	sc.mu.Unlock()

	_,_,ok:=sc.rf.Start(*op)
	defer func() {
		sc.mu.Lock()
		delete(sc.OpMap,AddIndex(int(op.ClientId), uint64(op.RequestId)))
		sc.mu.Unlock()
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


func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := &Op{
		ClientId:       args.ClientID,
		RequestId:      args.RequestID,
		OpType:         OpType("Leave"),
		Time:           time.Now().UnixMilli(),
	}
	term:=0
	isLeader:=false
	reply.WrongLeader=false

	
	sc.mu.Lock()
	if term,isLeader=sc.rf.GetState();!isLeader{
		sc.mu.Unlock()
		return
	}
	
	
	//如果client已经发起过请求，并且已经成功提交，则return
	if lastRequestId, ok := sc.LastRequestidMap[int(op.ClientId)]; ok && lastRequestId >= op.RequestId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	
	OpContent:=NewOpContext(op,term)

	sc.OpMap[OpContent.index]=OpContent

	sc.mu.Unlock()

	_,_,ok:=sc.rf.Start(*op)
	defer func() {
		sc.mu.Lock()
		delete(sc.OpMap,AddIndex(int(op.ClientId), uint64(op.RequestId)))
		sc.mu.Unlock()
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

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := &Op{
		ClientId:       args.ClientID,
		RequestId:      args.RequestID,
		OpType:         OpType("Move"),
		Time:           time.Now().UnixMilli(),
	}
	term:=0
	isLeader:=false
	reply.WrongLeader=false

	
	sc.mu.Lock()
	if term,isLeader=sc.rf.GetState();!isLeader{
		sc.mu.Unlock()
		return
	}
	
	
	//如果client已经发起过请求，并且已经成功提交，则return
	if lastRequestId, ok := sc.LastRequestidMap[int(op.ClientId)]; ok && lastRequestId >= op.RequestId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	
	OpContent:=NewOpContext(op,term)

	sc.OpMap[OpContent.index]=OpContent

	sc.mu.Unlock()

	_,_,ok:=sc.rf.Start(*op)
	defer func() {
		sc.mu.Lock()
		delete(sc.OpMap,AddIndex(int(op.ClientId), uint64(op.RequestId)))
		sc.mu.Unlock()
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

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := &Op{
		ClientId:       args.ClientID,
		RequestId:      args.RequestID,
		OpType:         OpType("Query"),
		Time:           time.Now().UnixMilli(),
	}
	term:=0
	isLeader:=false
	reply.WrongLeader=false

	
	sc.mu.Lock()
	if term,isLeader=sc.rf.GetState();!isLeader{
		sc.mu.Unlock()
		return
	}

	
	
	//如果client已经发起过请求，并且已经成功提交，则return
	if lastRequestId, ok := sc.LastRequestidMap[int(op.ClientId)]; ok && lastRequestId >= op.RequestId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	
	OpContent:=NewOpContext(op,term)

	sc.OpMap[OpContent.index]=OpContent

	sc.mu.Unlock()

	_,_,ok:=sc.rf.Start(*op)
	defer func() {
		sc.mu.Lock()
		delete(sc.OpMap,AddIndex(int(op.ClientId), uint64(op.RequestId)))
		sc.mu.Unlock()
	}()
	
	if !ok {
		return
	}

	select {
	case <-OpContent.Waitch:
		reply.Err = OK
		if args.Num >= 0 && args.Num < len(sc.configs) {
			reply.Config = sc.configs[args.Num]
		} else {
			reply.Config = sc.configs[len(sc.configs)-1]
		}
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.applyStateMachineLoop()
	return sc
}

func(sc *ShardCtrler) applyStateMachineLoop(){
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				func() {
					sc.mu.Lock()
					defer sc.mu.Unlock()
					op := applyMsg.Command.(Op)

					if op.RequestId <= sc.LastRequestidMap[int(op.ClientId)] {
						return
					}
					val := Config{}

					switch op.OpType {
					case OpJoin:
						sc.version++
						config := sc.executeJoin(op.Groups)
						sc.configs = append(sc.configs, config)
						sc.LastRequestidMap[int(op.ClientId)] = op.RequestId
					case OpLeave:
						sc.version++
						config := sc.executeLeave(op.Gids)
						sc.configs = append(sc.configs, config)
						sc.LastRequestidMap[int(op.ClientId)] = op.RequestId
					case OPMove:
						sc.version++
						config := sc.executeMove(op.Shard, op.Gid)
						sc.configs = append(sc.configs, config)
						sc.LastRequestidMap[int(op.ClientId)] = op.RequestId
						
					case OPQuery:
						val = sc.executeQuery(op.Version)
					}
					if c, ok := sc.OpMap[AddIndex(int(op.ClientId),uint64(op.RequestId))]; ok {
						c.Waitch <- val
					}
				}()
			}
		}
	}
}

func (sc *ShardCtrler)executeJoin(Groups map[int][]string) Config{
	length := len(sc.configs)
    lastConfig := sc.configs[length-1]
    newGroups := GroupCopy(lastConfig.Groups)

    newConfig := Config{
        Num:    length,
        Shards: [NShards]int{},
        Groups: newGroups,
    }

	gids := make([]int, 0)
	for gid, _ := range lastConfig.Groups {
		gids = append(gids, gid)
	}
	for gid, _:= range Groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})

	var shards [NShards]int
	copy(shards[:], lastConfig.Shards[:])

	for i := 0; i < len(shards); i++ {
		shards[i] = gids[i%len(gids)]
	}
	newConfig.Shards=shards
	
	return newConfig
}

func (sc *ShardCtrler)executeLeave(GIDs []int) Config{
	length := len(sc.configs)
    lastConfig := sc.configs[length-1]
    newGroups := GroupCopy(lastConfig.Groups)

    newConfig := Config{
        Num:    length,
        Shards: [NShards]int{},
        Groups: newGroups,
    }

	for _, gid := range GIDs {
		delete(newGroups, gid)
	}
	gids := make([]int, 0)
	for gid, _ := range newGroups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})

	var shards [NShards]int
	if len(gids) == 0 {
		shards = [NShards]int{}
	} else {
		for i := 0; i < len(shards); i++ {
			shards[i] = gids[i%len(gids)]
		}
	}

	newConfig.Shards=shards
	return newConfig
}



func (sc *ShardCtrler)executeMove(shard int, gid int) Config{
	length := len(sc.configs)
    lastConfig := sc.configs[length-1]

    newGroups := GroupCopy(lastConfig.Groups)

    newConfig := Config{
        Num:    length,
        Shards: lastConfig.Shards,
        Groups: newGroups,
    }

    newConfig.Shards[shard] = gid

    return newConfig
}
func (sc *ShardCtrler)executeQuery(num int) Config{
	length := len(sc.configs)
    config := Config{}
    if num == -1 || num >= length {
        config = sc.configs[length-1]
    } else {
        config = sc.configs[num]
    }

    newGroups := GroupCopy(config.Groups)

    newConfig := Config{
        Num:    config.Num,
        Shards: config.Shards,
        Groups: newGroups,
    }

    return newConfig
}
