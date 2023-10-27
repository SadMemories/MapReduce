package mr

import "os"
import "fmt"
import "log"
import "sort"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


type StatusWorker struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string

	workerId int // 指示当前worker的索引号

	mapOrReduce bool  // 用于指示当前worker执行的是map任务还是reduce任务 false为Map任务 true为Reduce任务
	shutdown bool  // 指示当前的worker是否已经关闭
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

func (worker *StatusWorker) getMapKeyValue(reply *MapTaskReply) []KeyValue {
	intermediate := []KeyValue{}
	log.Printf("reply: %+v\n", reply)
	file, err := os.Open(reply.FileName)  // map任务要读取的文件
	if err != nil {
		log.Printf("=============================\n")
		log.Printf("fileName: %s\n", reply.FileName)
		log.Fatalf("cannot open %s", reply.FileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := worker.mapf(reply.FileName, string(content))  // 掉用map函数进行处理
	intermediate = append(intermediate, kva...)  // kva...是将kva切片中的所有元素逐一追加到intermediate切片中

	return intermediate
}

func (worker *StatusWorker) getAllKeyValue(reply *ReduceTaskReply) []KeyValue {
	intermediate := []KeyValue{}

	for i := 0; i < reply.FileCount; i++ {
		tmpName := fmt.Sprintf("mr-%d-%d", i, reply.ReduceId)
		
		file, err := os.Open(tmpName)
		if err != nil {
			log.Fatalf("cannot open file...")
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	return intermediate
}

func (worker *StatusWorker) writeOutFile(reply *ReduceTaskReply, intermediate []KeyValue) {
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.ReduceId)
	ofile, err := os.CreateTemp("", "outTemp")
	if err != nil {
		log.Fatalf("cannot create temp file...")
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		output := worker.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("change temp Name err")
	}
}

func (worker *StatusWorker) writeReduceFile(reply *MapTaskReply, intermediate []KeyValue) {
	// 遍历intermediate中的键值对，对键使用ihash函数进行哈希，与reply.NReduce取余得到要存放的文件位置
	reduceKeyValList := make([][]KeyValue, reply.NReduce)  // 创建NReduce个子数组
	for _, kv := range intermediate {
		reduceIdx := ihash(kv.Key) % reply.NReduce
		reduceKeyValList[reduceIdx] = append(reduceKeyValList[reduceIdx], kv)
	}

	// log.Printf("NReduce: %d\n", reply.NReduce)
	for i := 0; i < reply.NReduce ; i++ {
		// 创建一个临时文件
		tempFile, err := os.CreateTemp("", "temp-")
		if err != nil {
			log.Fatalf("cannot create temp file...")
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range reduceKeyValList[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Encode %+v", kv)
			}
		}
		tempFile.Close()  // 关闭临时文件
		
		newName := fmt.Sprintf("mr-%d-%d", reply.FileId, i)
		err = os.Rename(tempFile.Name(), newName)
		if err != nil {
			log.Fatalf("change temp Name err")
		}
	}
}

func (worker *StatusWorker) delMapTask(reply *MapTaskReply) {

	args := DelTaskArgs{}
	args.FileId = reply.FileId
	args.WorkerId = reply.WorkerId

	delReply := DelTaskReply{}
	// log.Printf("The worker %d is deleting the completed Map Task via RPC call\n", reply.WorkerId)

	//调用call方法通过RPC请求task
	call("Coordinator.DelFromStartMapTask", &args, &delReply)

	if delReply.Accept {
		// log.Printf("Delete Map Task work success\n")
	} else {
		// log.Printf("Delete Map Task work failed\n")
	}
}

func (worker *StatusWorker) delReduceTask(reply *ReduceTaskReply) {

	args := DelTaskArgs{}
	args.FileId = reply.ReduceId
	args.WorkerId = reply.WorkerId

	delReply := DelTaskReply{}
	// log.Printf("The worker %d is deleting the completed Reduce Task via RPC call\n", reply.WorkerId)

	//调用call方法通过RPC请求task
	call("Coordinator.DelFromStartReduceTask", &args, &delReply)

	if delReply.Accept {
		// log.Printf("Delete Reduce Task work success\n")
	} else {
		// log.Printf("Delete Reduce Task work failed\n")
	}
}

func (worker *StatusWorker) handleMapTask(reply *MapTaskReply) {
	log.Printf("Worker %d is handling the map task...\n", reply.WorkerId)

	intermediate := worker.getMapKeyValue(reply)  // 获取map文件中的所有键值对
	// log.Printf("%s file content is handled, intermediate len is %d\n", reply.FileName, len(intermediate))

	worker.writeReduceFile(reply, intermediate)  // 将map任务生成的键值对保存到指定的中间文件中
	// log.Printf("Worker %d write reduce file complete...\n", reply.WorkerId)

	// 完成Map任务的处理，将已处理的Map任务从任务队列中移除
	// 在移除任务队列中的Map任务时应该通过RPC进行移除，因为任务队列是coordinator管理的
	worker.delMapTask(reply)
	log.Printf("Worker %d handle Map finished\n", reply.WorkerId)
	
	// time.Sleep(1 * time.Second)  // 方便调试加的睡眠时间
}

func (worker *StatusWorker) handleReduceTask(reply *ReduceTaskReply) {
	log.Printf("Worker %d is handling the Reduce task...\n", reply.WorkerId)

	intermediate := worker.getAllKeyValue(reply)  // 获取mr-X-ReduceId所有中间文件的KeyValue
	// log.Printf("Currently, the keys and values of all intermediate files mr-X-%d have been obtained...\n", reply.ReduceId)

	worker.writeOutFile(reply, intermediate)  // 将所有的intermediate键值对保存在out文件中
	// log.Printf("The output of the reduce task has been completed\n")

	worker.delReduceTask(reply)
	log.Printf("Worker %d handle Reduce finished\n", reply.WorkerId)
}

func (worker *StatusWorker) process() {

	// 索要Map任务
	if !worker.mapOrReduce {
		reply := worker.askMapTask() // 通过rpc请求获取Map任务, reply是一个指针类型

		if reply == nil {  // 说明此时所有的map任务已经完成，应该开始执行reduce任务
			worker.mapOrReduce = true
		} else {
			// reply中有一个状态表示当前是否有可执行的map任务，因为当前所有的map任务可能正在被其他worker执行，
			// 此时当前的worker应该等待，因为其他的worker可能因为某些原因执行失败，因此有两个选择：
			// 1. 没有map任务可以执行，当前worker等待
			// 2. 有map任务可执行，直接执行map任务
			if reply.FileId == -1 {  // worker等待
				// do Nothing，外层有一个for循环在等待
			} else {
				// 执行map任务
				worker.handleMapTask(reply)
			}
		}
	}

	// 索要Reduce任务
	if worker.mapOrReduce {
		reply := worker.askReduceTask()  // 通过rpc请求获取Reduce任务

		if reply == nil {  // 所有的reduce任务都已经完成，应该执行关闭
			worker.shutdown = true
		} else {
			// reply中有一个状态表示当前是否有可执行的reduce任务，因为当前所有的reduce任务可能正在被其他worker执行，
			// 此时当前的worker应该等待，因为其他的worker可能因为某些原因执行失败，因此有两个选择：
			// 1. 没有reduce任务可以执行，当前worker等待
			// 2. 有reduce任务可执行，直接执行map任务
			if reply.ReduceId == -1 {  // 等待

			} else {
				worker.handleReduceTask(reply)
			}
		}
	}
}

func (worker *StatusWorker) askMapTask() *MapTaskReply{
	args := MapTaskArgs{}
	args.WorkerId = worker.workerId  // 当前id还没有分配
	reply := MapTaskReply{}

	// 日志记录
	// log.Printf("worker %d is asking Map Task...\n", worker.workerId)

	//调用call方法通过RPC请求task
	call("Coordinator.GiveMapTask", &args, &reply)
	worker.workerId = reply.WorkerId  // 更新workerId

	if reply.AllFininshed {  // 所有的Map任务已经完成
		// log.Printf("All Map task is finished...\n")
		return nil
	}

	if reply.FileId == -1 {  // 当前所有的map任务都在处理中，worker需要等待一段时间
		// log.Printf("All map task is running, Worker %d is waiting...\n", reply.WorkerId)
		return &reply
	}

	// log.Printf("worker %d will handle Map task， File ID is %v, File Name is %v...\n", worker.workerId, reply.FileId, reply.FileName)
	return &reply
}

func (worker *StatusWorker) askReduceTask() *ReduceTaskReply{
	// 定义args请求参数，请求参数包括：1.WorkerId
	// 定义reply响应参数，响应参数包括：1.ReduceId(表示处理哪一个reduce任务) 2.AllReduceFin(所有的Reduce是否全部完成) 3.WorkerId
	args := ReduceTaskArgs{}
	args.WorkerId = worker.workerId
	reply := ReduceTaskReply{}

	// 日志记录
	// log.Printf("Worker %d is asking Reduce Task...\n", worker.workerId)
	call("Coordinator.GiveReduceTask", &args, &reply)  // 请求Reduce Task
	worker.workerId = reply.WorkerId  // 如果开启的线程过多，可能有的线程一开始就要执行Reduce任务
	
	if reply.AllReduceFin {  // 所有的Reduce任务都已经全部完成
		// log.Printf("All reduce tasks have been completed\n")
		return nil
	}

	if reply.ReduceId == -1 {  // 所有的Reduce任务都在处理中，当前任务需要等待
		// log.Printf("All Reduce task is running, Worker %d is waiting...\n", reply.WorkerId)
		return &reply
	}

	log.Printf("Worker %d will handle Reduce task， Reduce ID is %v...\n", worker.workerId, reply.ReduceId)
	return &reply
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 向coordinator.go发送RPC请求一个task，获得一个未处理的文件

	// 读取未处理的文件内容，并调用map函数，产生intermediate结果，将结果写入一个文件中，文件命名为mr-X-Y
	// 写文件的时候采用encoding/json格式
	// 对于某一个key的处理，通过ihash(key)选定哪一个reduce worker来进行处理
	// worker可能有部分时间处于等待，因为所有的reduce worker可能都在处理map，可以使用time.Sleep()进行周期性的询问

	// Your worker implementation here.

	worker := StatusWorker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.mapOrReduce = false
	worker.shutdown = false
	worker.workerId = -1

	// 打印一下日志
	// log.Printf("initialized\n")

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for !worker.shutdown {  // 只要worker没有关闭，则一直向master请求任务
		worker.process();  // 索要任务并处理任务
		// log.Printf("worker %d handle one time...\n", worker.workerId)
	}

	// log.Printf("worker shutdown\n")

}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}
