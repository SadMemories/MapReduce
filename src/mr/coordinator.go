package mr

import "log"
import "net"
import "os"
import "time"
import "net/rpc"
import "net/http"
import "sync"


type Coordinator struct {
	// Your definitions here.

	files []string  // 待处理的文件
	nReduce int   // map产生的中间文件数量
	workerId int  // 为worker分配的workerId
	maxHanleTime int64  // worker处理map任务最长的时间

	// 任务队列
	// 未开始的Map任务的任务队列
	// 已开始但未完成Map任务的任务队列
	// 一把针对Map任务的互斥锁
	// 未开始的Reduce的任务队列
	// 已开始但未完成Reduce任务的任务队列
	// 一把针对Reduce任务的互斥锁
	unstartMapTask *BlockQueue
	startMapTask *MapSet
	mapMutex sync.Mutex

	unstartReduceTask *BlockQueue
	startReduceTask *MapSet
	reduceMutex sync.Mutex

	mapStat []MapStat
	reduceStat []ReduceStat
	// 一个结构体数组指示每个file的处理情况(其实也就对应着每个map任务)，包括：1. 哪一个worker在处理  2. 处理的开始时间  3. 是否处理完成
	// 一个结构体数组指示每个reduce任务的处理情况，对应map过程后产生的中间文件，包括：1. 哪一个worker在处理 2. 处理的开始时间 3. 是否处理完成

	mapAllFinished bool  // 是否所有的map任务都处理完毕
	reduceAllFinished bool  // 是否所有的reduce任务都处理完毕

}

type MapStat struct {
	beginTime int64
	workerId int
}

type ReduceStat struct {
	beginTime int64
	workerId int
}

// map任务的回复，其中应该包括文件名，文件ID(文件ID用于指示当前是否有文件)
type MapTaskReply struct {
	FileName string
	FileId int
	NReduce int 
	AllFininshed bool  // 指示Map任务是否全部完成
	WorkerId int
}

// map任务的请求参数，目前还不知道填什么
type MapTaskArgs struct {
	WorkerId int
}

type DelTaskArgs struct {
	FileId int
	WorkerId int 
}

type DelTaskReply struct {
	Accept bool
}

type ReduceTaskReply struct {
	ReduceId int
	AllReduceFin bool
	WorkerId int
	FileCount int
}

type ReduceTaskArgs struct {
	WorkerId int
}

// Your code here -- RPC handlers for the worker to call.
// 实现一个RPC处理函数，给RPC请求响应一个文件名称
// 涉及到并行处理，需要上锁
// coordinator需要检测已经crash的worker，将任务分配给另一个worker

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func finishMapTaskReply(reply *MapTaskReply) {
	reply.AllFininshed = true
	reply.FileId = -1
}

func finishReduceTaskReply(reply *ReduceTaskReply) {
	reply.AllReduceFin = true
	reply.ReduceId = -1
}

func (c *Coordinator) PushReduceTaskInQueue() {
	for i := 0 ; i < c.nReduce; i++ {  // go中不支持前置自增的用法
		// log.Printf("putting %vth reduce task into Queue\n", i)
		c.unstartReduceTask.PutFront(i)
	}
}

// 从startMapTask中删除相应的Map任务
func (c *Coordinator) DelFromStartMapTask(args *DelTaskArgs, reply *DelTaskReply) error {
	
	// log.Printf("The file %d task processed by the worker %d is currently being deleted...\n", args.FileId, args.WorkerId)
	
	c.mapMutex.Lock()
	beginTime := c.mapStat[args.FileId].beginTime
	nowTime := time.Now().UnixMilli()
	savedWorkerId := c.mapStat[args.FileId].workerId

	if !c.startMapTask.Find(args.FileId) {
		c.mapMutex.Unlock()
		log.Printf("startMapTask donot have %d Task, delete failed\n", args.FileId)
		reply.Accept = false
		return nil
	}

	if savedWorkerId != args.WorkerId {
		c.mapMutex.Unlock()
		log.Printf("The file %d is not processed by the worker numbered %d, but by the worker numbered %d...\n", args.FileId, args.WorkerId, savedWorkerId)
		reply.Accept = false
		return nil
	}

	if nowTime - beginTime > c.maxHanleTime {  // 超过了处理时间
		c.mapMutex.Unlock()
		log.Printf("Processing time exceeded, deletion failed...\n")
		reply.Accept = false
		return nil
	}

	isSuccess := c.startMapTask.Remove(args.FileId)
	if isSuccess {
		c.mapMutex.Unlock()
		log.Printf("delete success\n")
		reply.Accept = true
		return nil
	} else {
		c.mapMutex.Unlock()
		log.Printf("delete failed\n")
		reply.Accept = false
		return nil
	}

}

// 从startMapTask中删除相应的Reduce任务
func (c *Coordinator) DelFromStartReduceTask(args *DelTaskArgs, reply *DelTaskReply) error {
	
	log.Printf("The Reduce %d task processed by the worker %d is currently being deleted...\n", args.FileId, args.WorkerId)
	
	c.reduceMutex.Lock()
	beginTime := c.reduceStat[args.FileId].beginTime
	nowTime := time.Now().UnixMilli()
	savedWorkerId := c.reduceStat[args.FileId].workerId

	if !c.startReduceTask.Find(args.FileId) {
		c.reduceMutex.Unlock()
		// log.Printf("startReduceTask donot have %d Task, delete failed\n", args.FileId)
		reply.Accept = false
		return nil
	}

	if savedWorkerId != args.WorkerId {
		c.reduceMutex.Unlock()
		// log.Printf("The Reduce %d is not processed by the worker numbered %d, but by the worker numbered %d...\n", args.FileId, args.WorkerId, savedWorkerId)
		reply.Accept = false
		return nil
	}

	if nowTime - beginTime > c.maxHanleTime {
		c.reduceMutex.Unlock()
		// log.Printf("Processing time exceeded, deletion failed...\n")
		reply.Accept = false
		return nil
	}

	isSuccess := c.startReduceTask.Remove(args.FileId)
	if isSuccess {
		c.reduceMutex.Unlock()
		// log.Printf("delete success\n")
		reply.Accept = true
	} else {
		c.reduceMutex.Unlock()
		// log.Printf("delete failed\n")
		reply.Accept = false
	}

	return nil
}

func (c *Coordinator) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {

	if args.WorkerId == -1 {
		reply.WorkerId = c.workerId
		c.workerId++
	} else {
		reply.WorkerId = args.WorkerId
	}

	// log.Printf("Worker %d is asking Reduce task...\n", reply.WorkerId)

	// 判断当前任务队列中的Reduce任务是否全部处理完毕
	// 使用互斥锁锁定，因为要判断unstartReduceTask和startReduceTask是否全部为0
	c.reduceMutex.Lock()

	// 所有的Map任务都已经处理完毕
	if c.reduceAllFinished || (c.unstartReduceTask.Size() == 0 && c.startReduceTask.Size() == 0) {
		// 将Coordinator中的mapAllFinished置为true
		c.reduceAllFinished = true
		c.reduceMutex.Unlock()
		// log.Printf("Worker %d : All Reduce task is finished...\n", reply.WorkerId)
		// 将reply中的AllReduceFin置为true，并将ReduceId置为-1
		finishReduceTaskReply(reply)
		return nil
	}

	c.reduceMutex.Unlock()

	// 执行到这里说明map任务没有全部处理完成
	reduceId, err := c.unstartReduceTask.PopFront()  // 原子操作
	if err != nil {  // 任务为空
		reply.ReduceId = -1
		// log.Printf("Worker %d : All Reduce task is running, please wait a moment...\n", reply.WorkerId)
		return nil
	}

	// 有任务
	reply.ReduceId = reduceId.(int)
	reply.AllReduceFin = false
	reply.FileCount = len(c.files)
	c.reduceMutex.Lock()
	c.reduceStat[reduceId.(int)].beginTime = time.Now().UnixMilli()  // 获取毫秒时间
	c.reduceStat[reduceId.(int)].workerId = reply.WorkerId
	c.startReduceTask.Insert(reduceId.(int))
	c.reduceMutex.Unlock()
	// log.Printf("Worker %d : get Reduce task, ReduceId is %d\n", reply.WorkerId, reply.ReduceId)

	return nil
}

// 为worker分配Map任务
func (c *Coordinator) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	
	if args.WorkerId == -1 {
		reply.WorkerId = c.workerId
		c.workerId++
	} else {
		reply.WorkerId = args.WorkerId
	}

	// log.Printf("Worker %d is asking Map task...\n", reply.WorkerId)
	// 判断当前任务队列中的map任务是否全部处理完毕
	// 使用互斥锁锁定，因为要判断unstartMapTask和startMapTask是否全部为0
	c.mapMutex.Lock()

	if c.mapAllFinished {
		c.mapMutex.Unlock()
		// log.Printf("Worker %d : All Map task is finished...\n", reply.WorkerId)
		finishMapTaskReply(reply)
		return nil
	}

	// 所有的Map任务都已经处理完毕
	if c.unstartMapTask.Size() == 0 && c.startMapTask.Size() == 0 {
		// 将Coordinator中的mapAllFinished置为true
		c.mapAllFinished = true
		c.mapMutex.Unlock()
		// log.Printf("Worker %d : All Map task is finished..., initialized Reduce Queue\n", reply.WorkerId)
		// 将reply中的allFinished置为true，并将fileId置为-1
		finishMapTaskReply(reply)
		// 在第一个worker判断出当前所有的map任务都已经处理完毕之后，还需要将所有的reduce任务加入到任务队列中
		c.PushReduceTaskInQueue()
		return nil
	}

	c.mapMutex.Unlock()
	
	// 执行到这里说明map任务没有全部处理完成
	fileId, err := c.unstartMapTask.PopFront()  // 原子操作
	if err != nil {  // 任务为空
		reply.FileId = -1
		// log.Printf("Worker %d : All Map task is running, please wait a moment...\n", reply.WorkerId)
		return nil
	}


	// 有任务
	reply.FileId = fileId.(int)
	reply.FileName = c.files[fileId.(int)]
	reply.AllFininshed = false
	reply.NReduce = c.nReduce
	c.mapMutex.Lock()
	c.mapStat[fileId.(int)].beginTime = time.Now().UnixMilli()  // 获取毫秒时间
	c.mapStat[fileId.(int)].workerId = reply.WorkerId
	c.startMapTask.Insert(fileId.(int))
	c.mapMutex.Unlock()
	log.Printf("Worker %d : get Map task, fileName is %s, fileId is %d...\n", reply.WorkerId, reply.FileName, reply.FileId)

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)  // 注册Coordinator对应的方法，这里对应的Example方法
	rpc.HandleHTTP()  // 注册HTTP路由，
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.reduceAllFinished {
		log.Printf("server ends running\n")
		ret = true
	}

	return ret
}

func (c *Coordinator) CheckAndSetMapTask() {
	for fileId, isHandle := range c.startMapTask.mapBool {
		if isHandle {  // isHandle为true说明当前任务正在被一个worker处理
			beginTime := c.mapStat[fileId.(int)].beginTime
			workerId := c.mapStat[fileId.(int)].workerId
			nowTime := time.Now().UnixMilli()

			if nowTime - beginTime > c.maxHanleTime {  // 超出了最长处理时间
				log.Printf("The fileId %d task processed by worker %d has expired and is put back into Map task\n", fileId.(int), workerId)
				isSuccess := c.startMapTask.Remove(fileId.(int))
				if isSuccess {
					c.unstartMapTask.PutFront(fileId)
				}
				
			}
		}
	}
}

func (c *Coordinator) CheckAndSetReduceTask() {
	for reduceId, isHandle := range c.startReduceTask.mapBool {
		if isHandle {  // isHandle为true说明当前任务正在被一个worker处理
			beginTime := c.reduceStat[reduceId.(int)].beginTime
			// workerId := c.reduceStat[reduceId.(int)].workerId
			nowTime := time.Now().UnixMilli()

			if nowTime - beginTime > c.maxHanleTime {  // 超出了最长处理时间
				// log.Printf("The reduceId %d task processed by worker %d has expired and is put back into Reduce task\n", reduceId.(int), workerId)
				isSuccess := c.startReduceTask.Remove(reduceId.(int))
				if isSuccess {
					c.unstartReduceTask.PutFront(reduceId)
				}
			}
		}
	}
}

// 周期性的检查是否有map任务过期
func (c *Coordinator) CheckUnfinishedMapAndReduceTesk() {
	for !c.reduceAllFinished {  // 死循环
		// log.Printf("Start a new round of task inspection work\n")
		c.mapMutex.Lock()
		c.CheckAndSetMapTask()
		c.mapMutex.Unlock()

		c.reduceMutex.Lock()
		c.CheckAndSetReduceTask()
		c.reduceMutex.Unlock()

		time.Sleep(2 * time.Second)  // 睡眠10s再进行检查
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.workerId = 0
	c.maxHanleTime = 10000  // 10秒 10000毫秒
	c.unstartMapTask = NewBlockQueue()
	c.startMapTask = NewMapSet()
	c.unstartReduceTask = NewBlockQueue()
	c.startReduceTask = NewMapSet()
	c.mapStat = make([]MapStat, len(files))
	c.reduceStat = make([]ReduceStat, nReduce)
	c.mapAllFinished = false
	c.reduceAllFinished = false

	c.server()
	// log.Printf("coordinator starts listening\n")
	log.Printf("Tesk number: %d\n", len(files))

	go c.CheckUnfinishedMapAndReduceTesk()  // 开启一个协程来周期性检查Map或者Reduce任务是否已经完成

	for i := 0; i < len(files); i++ {
		// log.Printf("file %v is pushed in BlockQueue", i)
		c.unstartMapTask.PutFront(i)
	}
	// log.Printf("unstartMapTask count: %d\n", c.unstartMapTask.Size())
	return &c
}


// 深圳-上海 530
// 上海-深圳 488