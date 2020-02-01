package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	Call(mapf, reducef)
}

func Call(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		time.Sleep(time.Second)
		args := ReqArgs{
			Type: GetJob,
		}
		reply := Reply{}
		ok := call("Master.Handler", &args, &reply)
		if !ok {
			continue
		}
		switch reply.Type {
		case MapJobType:
			fmt.Printf("Get a map job, index: %d, filenames: %v\n", reply.Index, reply.Filenames)
			intermediates := make([][]KeyValue, reply.ReduceNum)
			for i := 0; i < reply.ReduceNum; i++ {
				intermediates[i] = make([]KeyValue, 0)
			}
			for _, f := range reply.Filenames {
				file, err := os.Open(f)
				if err != nil {
					return
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					return
				}
				kva := mapf(f, string(content))
				for _, kv := range kva {
					hash := ihash(kv.Key) % reply.ReduceNum
					kvl := intermediates[hash]
					kvl = append(kvl, kv)
					intermediates[hash] = kvl
				}
			}
			for i := 0; i < reply.ReduceNum; i++ {
				imName := fmt.Sprintf("mr-%v-%v", reply.Index, i)
				ofile, _ := os.Create(imName)
				kva := intermediates[i]
				for _, kv := range kva {
					fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}
			doneArgs := ReqArgs{
				Type:  AckWorkDone,
				Index: reply.Index,
			}
			doneReply := Reply{}
			call("Master.Handler", &doneArgs, &doneReply)
		case ReduceJobType:
			fmt.Printf("Get a reduce job, index: %d, filenames: %v\n", reply.Index, reply.Filenames)
			index := reply.Index
			intermediate := make([]KeyValue, 0)
			for _, f := range reply.Filenames {
				file, _ := os.Open(f)
				buf := bufio.NewReader(file)
				for {
					line, err := buf.ReadString('\n')
					if err != nil || io.EOF == err {
						break
					}
					kv := KeyValue{}
					fmt.Sscanf(line, "%v %v", &kv.Key, &kv.Value)
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%v", index)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()
			doneArgs := ReqArgs{
				Type:  AckWorkDone,
				Index: reply.Index,
			}
			doneReply := Reply{}
			call("Master.Handler", &doneArgs, &doneReply)
		case WaitType:
			continue
		case ExitType:
			return
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
