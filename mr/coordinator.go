package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// one coordinator is created, they are for coordinating the workers and assigning tasks
	mapFinished    bool
	mapLength      int // does not change
	reduceFinished bool
	//unfinishedFilenames []string
	finishedMapTaskNums    []int // keeps track of finished map task nums for timer when CHECKING to see if it should assign to another work AND after it completes to see if it was faster too
	mapTasks               []Task
	finishedReduceTaskNums []int // keeps track for reduce tasks
	reduceTasks            []Task
	reduceNum              int // val does not change, do not need lock when accessing
	lock                   *sync.Mutex
}

type Task struct {
	TaskType int // 0 = map, 1 = reduce
	TaskNum  int
	Files    []string //only 1 file if map task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TaskFinder(args *TaskFinderArgs, reply *TaskFinderReply) error {
	// gives the worker an available task.
	// if map phase: give next avail map task. if none then return -1 to TaskNumber

	// quick check to see if all map tasks are done

	c.lock.Lock()
	if c.mapLength == len(c.finishedMapTaskNums) {
		c.mapFinished = true
	}
	if c.reduceNum == len(c.finishedReduceTaskNums) {
		c.reduceFinished = true
	}
	c.lock.Unlock()

	c.lock.Lock()
	if !c.mapFinished {
		// assign map task
		if len(c.mapTasks) > 0 {
			// still map tasks to do, takes 1st task
			firstTask := c.mapTasks[0]
			var tmp []string
			tmp = append(tmp, firstTask.Files[0])
			reply.FileName = tmp
			reply.TaskNumber = firstTask.TaskNum
			reply.TaskType = firstTask.TaskType
			reply.ReduceNum = c.reduceNum
			c.mapTasks = c.mapTasks[1:]

			c.lock.Unlock()
			//CALL GO ROUTINE here for timing task
			go c.leTimer(firstTask)

			return nil
		} else {
			// NO map tasks to do, tell worker to do nothing (until it calls again and finds out mapFinished = true or finds task)
			c.lock.Unlock()
			var tmp []string
			reply.FileName = tmp
			reply.TaskNumber = -1
			reply.TaskType = -1
			reply.ReduceNum = -1
			return nil
		}

	} else if !c.reduceFinished {
		// assign reduce task

		if len(c.reduceTasks) > 0 {
			// still have reduce tasks to do, takes 1st task
			firstTask := c.reduceTasks[0]

			// set files to reduce all the ones with reduce number = firstTask.TaskNum
			if len(firstTask.Files) == 0 { // this bug took an hr to find, MUST see if already files created incase of failure earlier
				for index := 0; index < len(c.finishedMapTaskNums); index++ {
					strBuilder := "./mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(firstTask.TaskNum)
					firstTask.Files = append(firstTask.Files, strBuilder)
				}
			}
			reply.FileName = firstTask.Files
			reply.TaskNumber = firstTask.TaskNum
			reply.TaskType = firstTask.TaskType
			reply.ReduceNum = c.reduceNum
			c.reduceTasks = c.reduceTasks[1:]

			c.lock.Unlock()

			//CALL GO ROUTINE here for timing task
			go c.leTimer(firstTask)

			return nil
		} else {
			// NO reduce tasks to do
			c.lock.Unlock()
			var tmp []string
			reply.FileName = tmp
			reply.TaskNumber = -1
			reply.TaskType = -1
			reply.ReduceNum = -1
			return nil
		}

	} else {
		// being here means map and reduce task is done
		c.lock.Unlock()
		var tmp []string
		reply.FileName = tmp
		reply.TaskNumber = -1
		reply.TaskType = -1
		reply.ReduceNum = -1
		return nil

	}

}

func (c *Coordinator) leTimer(task Task) {
	//works with both map or reduce tasks
	// sleep 10 seconds
	// IF time is up, must delete the file previous worker was working on
	time.Sleep(10 * time.Second)

	// if task still not done, then give job to another worker (simply put task back)
	c.lock.Lock()
	if task.TaskType == 0 {

		found := false
		for _, element := range c.finishedMapTaskNums {
			if element == task.TaskNum {
				found = true
			}
		}
		if !found {
			c.mapTasks = append(c.mapTasks, task)
		}
	} else {
		found := false
		for _, element := range c.finishedReduceTaskNums {
			if element == task.TaskNum {
				found = true
			}
		}
		if !found {
			c.reduceTasks = append(c.reduceTasks, task)
		}
	}

	c.lock.Unlock()
}

func (c *Coordinator) MapTaskFinished(args *MapTaskFinishedArgs, reply *MapTaskFinishedReply) error {
	// sets map task to done
	// note: not checking to see if it IS ALREADY done bc the output should be the same anyways
	// after some testing, appears the file with same name remains there, later one replaces it, should be SAME OUTPUT so does not matter

	c.lock.Lock()
	if len(c.finishedMapTaskNums) < c.mapLength { // ensures duplicate map task finishing will not the finished ones
		c.finishedMapTaskNums = append(c.finishedMapTaskNums, args.TaskNumber)
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) ReduceTaskFinished(args *ReduceTaskFinishedArgs, reply *ReduceTaskFinishedReply) error {
	// sets reduce task to done
	// note: not checking to see if it is already done in case where it's too slow,

	c.lock.Lock()
	if len(c.finishedReduceTaskNums) < c.reduceNum { // same as map reasoning
		c.finishedReduceTaskNums = append(c.finishedReduceTaskNums, args.TaskNumber)
	}

	c.lock.Unlock()
	return nil
}

func (c *Coordinator) IsReduceFinished(args *IsReduceFinishedArgs, reply *IsReduceFinishedReply) error {
	c.lock.Lock()
	if c.reduceFinished {
		reply.Finished = true
	} else {
		reply.Finished = false
	}
	c.lock.Unlock()
	return nil
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("RPC HANDLED")
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.Lock()
	if c.reduceFinished {
		ret = true
	}
	c.lock.Unlock()

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// creation of map tasks
	var allMapTasks []Task
	for index, file := range files {
		var tmp []string
		tmp = append(tmp, file)
		allMapTasks = append(allMapTasks, Task{0, index, tmp})
	}

	// creation of empty reduce tasks
	var allReduceTasks []Task
	for index := 0; index < nReduce; index++ {
		var tmp []string
		allReduceTasks = append(allReduceTasks, Task{1, index, tmp})
	}

	var empty []int

	c := Coordinator{false, len(allMapTasks), false, empty, allMapTasks, empty, allReduceTasks, nReduce, &sync.Mutex{}} // set 1st to true after creating files for testing reduce

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
