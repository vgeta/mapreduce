package mapreduce
import "container/list"
import "fmt"
import "math/rand"
import "sync"

type WorkerInfo struct {
  address string
  mu sync.Mutex
// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
	registered := make(chan bool)
	go func() {
		var first bool
		first = true
		for {
			worker := <-mr.registerChannel
			fmt.Println("@ register master !", worker)
			if mr.Workers == nil{
				mr.Workers = make(map[string] *WorkerInfo)
			}
			mr.Workers[worker] = &WorkerInfo{worker, sync.Mutex{}}
			if first {
				first = false
				registered <- true
			}
		}
	}()
	<-registered
	var wg sync.WaitGroup
	wg.Add(mr.nMap)
	for nMaps := 0;nMaps < mr.nMap; nMaps++{
		var doJobArgs DoJobArgs
		doJobArgs.JobNumber = nMaps
		doJobArgs.File = mr.file
		doJobArgs.NumOtherPhase = mr.nReduce
		doJobArgs.Operation = Map
		var doJobReply DoJobReply
		go func() {
			for success := false;success == false;{
				idx := (rand.Int() % len(mr.Workers)) + 1
				for _,slave := range mr.Workers{
					idx -= 1
					if idx == 0{
						slave.mu.Lock()
						ok := call(slave.address, "Worker.DoJob", doJobArgs, &doJobReply)
						if ok && doJobReply.OK {
							success = true
							wg.Done()
						}
						slave.mu.Unlock()
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	wg.Add(mr.nReduce)
        for nReduces := 0;nReduces < mr.nReduce; nReduces++{
                var doJobArgs DoJobArgs
                doJobArgs.JobNumber = nReduces
                doJobArgs.File = mr.file
                doJobArgs.NumOtherPhase = mr.nMap
                doJobArgs.Operation = Reduce
                var doJobReply DoJobReply
                go func() {
                        for success := false;success == false;{
                                idx := (rand.Int() % len(mr.Workers)) + 1
                                for _,slave := range mr.Workers{
                                        idx -= 1
                                        if idx == 0{
                                                slave.mu.Lock()
                                                ok := call(slave.address, "Worker.DoJob", doJobArgs, &doJobReply)
                                                if ok && doJobReply.OK {
                                                        success = true
                                                        wg.Done()
                                                }
                                                slave.mu.Unlock()
                                                break
                                        }
                                }
                        }
                }()
        }
        wg.Wait()
	return mr.KillWorkers()
}
