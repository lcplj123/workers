/*
*  Golang并发控制
*  Job/Worker 模式解析
 */

package workers

import (
	"fmt"
	"sync/atomic"
)

//  首先定义一个job的接口，只要实现了Do这个接口，就可以用Job表示
type Job interface {
	Do() error
}

// 定义job队列(每个worker一个，用来接收Job的)
type jobChan chan Job

// 全局Job队列 所有job都先入此队列
var thisJobQueue jobChan

var waitJob uint64 = 0 //等待被处理的job数
var doneJob uint64 = 0 //已处理的job数

// 每个worker都有一个job队列，在启动worker的时候会被注册到workerpool中
// 启动后通过自身的job队列取到job并执行job。
type worker struct {
	Index uint32 //worker的唯一索引
	//WorkerPool是一个全局变量，装载(维护)了每个worker的JobChannel（每个worker接收job的队列）
	WorkerPool chan jobChan
	//每个worker自身都维持一个jobchan队列，通过此队列来接收job
	JobChannel jobChan
	DoneJob    uint64 //本worker已处理的job
	quit       chan bool
}

func newWorker(index uint32, pool chan jobChan) *worker {
	return &worker{
		Index:      index,
		WorkerPool: pool,
		JobChannel: make(jobChan),
		quit:       make(chan bool),
		DoneJob:    0,
	}
}

func (w *worker) Start() {
	go func() {
		for {
			//每一次循环都向全局WorkerPool注册当前worker的jobchan
			//此处的精妙之处在于只要worker的JobChannel中有job，那么JobChan一定不在WorkerPool中
			//因为dispatcher中是先将WorkerPool中的JobChan取出来，然后在将Job放到JobChan中，此时JobChan就已经不在WorkerPool中了。
			w.WorkerPool <- w.JobChannel
			select {
			case job := <-w.JobChannel:
				if err := job.Do(); err != nil {
					fmt.Printf("execute job failed with err: %v", err)
				}
				atomic.AddUint64(&w.DoneJob, 1)
				atomic.AddUint64(&doneJob, 1)
			case <-w.quit:
				return
			}
		}
	}()
}

func (w *worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

//分发器，可以统计多项数据
type Dispatcher struct {
	WorkerPool chan jobChan
	Workers    []*worker
	quit       chan bool
	maxsize    int
}

//一个dispatcher对应一个总的job队列
func NewDispatcher(maxworkernum, maxjobqueue int) (*Dispatcher, jobChan) {
	if maxworkernum <= 0 {
		maxworkernum = 100 //默认100
	}
	if maxjobqueue < maxworkernum {
		maxjobqueue = maxworkernum
	}
	thisJobQueue = make(jobChan, maxjobqueue)
	return &Dispatcher{
		WorkerPool: make(chan jobChan, maxworkernum),
		Workers:    make([]*worker, 0, maxworkernum),
		quit:       make(chan bool),
		maxsize:    maxworkernum,
	}, thisJobQueue
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxsize; i++ {
		worker := newWorker(uint32(i), d.WorkerPool)
		d.Workers = append(d.Workers, worker)
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-thisJobQueue:
			//此处有可能生成无数的goroutine,可在此处进行控制
			go func(job Job) {
				atomic.AddUint64(&waitJob, 1)
				jobChan := <-d.WorkerPool
				jobChan <- job
				atomic.AddUint64(&waitJob, ^uint64(1-1))
			}(job)
		case <-d.quit:
			return
		}
	}
}

func (d *Dispatcher) Stop() {
	for _, worker := range d.Workers {
		worker.Stop()
	}
	d.quit <- true
}

func (d *Dispatcher) Stat() (uint64, uint64, uint64) {
	var sum uint64 = 0
	for _, worker := range d.Workers {
		sum += worker.DoneJob
	}
	return waitJob, doneJob, sum
}
