package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const ExecTimeout = 30

type MasterStatus int

const (
	Mapping MasterStatus = iota
	Reducing
	WorkDone
)

// JobRecord records the metadata of a map/reduce job
type JobRecord struct {
	Index     int
	Filenames []string
	Start     int64
	End       int64
}

// Master is mr master
type Master struct {
	// Your definitions here.
	JobRecords      []JobRecord
	MapNum          int
	ReduceNum       int
	ReduceLocations []string
	Lock            sync.Mutex
	Status          MasterStatus
}

func (m *Master) deliverJobs(reply *Reply) error {
	if m.Status == WorkDone {
		reply.Type = ExitType
		return nil
	}
	m.Lock.Lock()
	defer m.Lock.Unlock()
	now := time.Now().Unix()
	reply.ReduceNum = m.ReduceNum
	reply.Type = WaitType
	for i := range m.JobRecords {
		if m.JobRecords[i].Start == 0 || now-m.JobRecords[i].Start > ExecTimeout {
			m.JobRecords[i].Start = now
			reply.Type = MapJobType
			if m.Status == Reducing {
				reply.Type = ReduceJobType
			}
			reply.Index = m.JobRecords[i].Index
			reply.Filenames = m.JobRecords[i].Filenames
			break
		}
	}

	return nil
}

// workDone indicates the workers to exit
func (m *Master) workDone(args *ReqArgs, reply *Reply) error {
	fmt.Printf("Work done for index %d\n", args.Index)
	m.Lock.Lock()
	defer m.Lock.Unlock()
	// delete the finished one
	leftJobs := make([]JobRecord, 0)
	for _, r := range m.JobRecords {
		if r.Index != args.Index {
			leftJobs = append(leftJobs, r)
		}
	}
	m.JobRecords = leftJobs
	if len(leftJobs) == 0 {
		// produce reduce work if the mapping ends
		if m.Status == Mapping {
			for i := 0; i < m.ReduceNum; i++ {
				job := JobRecord{
					Index:     i,
					Filenames: make([]string, m.MapNum),
				}
				for j := 0; j < m.MapNum; j++ {
					job.Filenames[j] = fmt.Sprintf("mr-%v-%v", j, i)
				}
				m.JobRecords = append(m.JobRecords, job)
			}
		}
		m.Status++
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Handler(args *ReqArgs, reply *Reply) error {
	switch args.Type {
	case GetJob:
		if err := m.deliverJobs(reply); err != nil {
			return err
		}
	case AckWorkDone:
		if err := m.workDone(args, reply); err != nil {
			return err
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.Status == WorkDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// Split the files content to M copies
	m.JobRecords = make([]JobRecord, len(files))
	for i, f := range files {
		m.JobRecords[i] = JobRecord{
			Index:     i,
			Filenames: []string{f},
		}
	}
	m.MapNum = len(files)
	m.ReduceNum = nReduce

	m.server()
	return &m
}
