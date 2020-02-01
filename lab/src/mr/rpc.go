package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ReqMsgType int
type ReplyMsgType int

const (
	GetJob ReqMsgType = iota
	AckWorkDone
)

const (
	Ack ReplyMsgType = iota
	MapJobType
	ReduceJobType
	WaitType
	ExitType
	UnknownType
)

type ReduceJobData struct {
	DataLocation string
}

type ReqArgs struct {
	Type  ReqMsgType
	Index int
}

type Reply struct {
	Type      ReplyMsgType
	Index     int
	Filenames []string
	ReduceNum int
}
