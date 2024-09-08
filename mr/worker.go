package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	//CallExample()

	// keep looping for workers to check if theres work until reduce is done (whole operation is done)
	for !isReduceFinished() {

		fileNames, taskNumber, taskType, reduceNum := findNextTask()

		//fmt.Println(taskType)

		if taskType == 0 {
			// 0 means map task

			// for map, only one val in the list

			mapFile := fileNames[0]

			file, err := os.Open(mapFile)
			if err != nil {
				log.Fatalf("cannot open %v", mapFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", mapFile)
			}
			file.Close()
			kva := mapf(mapFile, string(content))

			// go through each kva pair and hash it + % reduce, in order to find which intermediate file it should be
			allKva := make([][]KeyValue, reduceNum) //2D slice, each index corresponds to which intermediate file

			for _, element := range kva {
				reduceTaskNum := ihash(element.Key) % reduceNum

				//for each key value pair, store it inside a row for the 2D array, row is based on which intermediate file it should be
				allKva[reduceTaskNum] = append(allKva[reduceTaskNum], element)
			}

			// go through each slice (intermediate file) and create JSON file for them

			for index, element := range allKva {
				fileStrBuilder := "./mr-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(index)

				ofile, _ := ioutil.TempFile("", "tempMap")
				defer ofile.Close()
				enc := json.NewEncoder(ofile)

				for _, kv := range element {
					err := enc.Encode(&kv)
					if err != nil {
						fmt.Println("ERROR ENCODING")
					}
				}
				os.Rename(ofile.Name(), fileStrBuilder)

			}

			//intermediate files have just been created now from map task!
			// tell coordinator it done
			// note: we are assuming that worker will NOT crash in the middle of writing these tiny tasks/creating the files

			call("Coordinator.MapTaskFinished", &MapTaskFinishedArgs{taskNumber}, &MapTaskFinishedReply{})
		} else if taskType == 1 {
			// reduce

			//go through each file and decode

			combinedKva := []KeyValue{}

			for _, file := range fileNames {

				actualFile, err := os.Open(file)
				if err != nil {
					fmt.Println("Error reading file")
					fmt.Println(err)
				}
				defer actualFile.Close()
				dec := json.NewDecoder(actualFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					combinedKva = append(combinedKva, kv)
				}

			}

			sort.Sort(ByKey(combinedKva))

			oname := "./mr-out-" + strconv.Itoa(taskNumber)

			ofile, _ := ioutil.TempFile("", "tempReduce")
			defer ofile.Close()

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(combinedKva) {
				j := i + 1
				for j < len(combinedKva) && combinedKva[j].Key == combinedKva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, combinedKva[k].Value)
				}
				output := reducef(combinedKva[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", combinedKva[i].Key, output)

				i = j
			}

			os.Rename(ofile.Name(), oname)

			//call coordinator to say it finished
			call("Coordinator.ReduceTaskFinished", &ReduceTaskFinishedArgs{taskNumber}, &ReduceTaskFinishedReply{})

		} else {
			// taskType must -1, sleep and then loop back to call again
			time.Sleep(1 * time.Second)
		}

	}

}

func isReduceFinished() bool {
	// returns true if whole process is done
	reply := IsReduceFinishedReply{}
	call("Coordinator.IsReduceFinished", &IsReduceFinishedArgs{}, &reply)
	return reply.Finished
}

func findNextTask() ([]string, int, int, int) {
	// finds the next task to map and returns the filename & task number
	args := TaskFinderArgs{}
	reply := TaskFinderReply{}
	call("Coordinator.TaskFinder", &args, &reply)
	return reply.FileName, reply.TaskNumber, reply.TaskType, reply.ReduceNum // if returns -1 to TaskType then that means no map tasks avail
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
