/*
*  Golang并发控制
*  Job/Worker 模式解析
 */

package workers

import "fmt"

//  首先定义一个job的接口，只要实现了Do这个接口，就可以用Job表示
type Job interface {
	Do() error
}

// 定义job队列(每个worker一个，用来接收Job的)
type JobChan chan Job

// 全局Job队列 所有job都先入此队列
var thisJobQueue JobChan

// 每个worker都有一个job队列，在启动worker的时候会被注册到workerpool中
// 启动后通过自身的job队列取到job并执行job。
type worker struct {
	//WorkerPool是一个全局变量，装载(维护)了每个worker的JobChannel（每个worker接收job的队列）
	WorkerPool chan JobChan
	//每个worker自身都维持一个jobchan队列，通过此队列来接收job
	JobChannel JobChan
	quit       chan bool
}

func newWorker(pool chan JobChan) *worker {
	return &worker{
		WorkerPool: pool,
		JobChannel: make(JobChan),
		quit:       make(chan bool),
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
	WorkerPool chan JobChan
	Workers    []*worker
	quit       chan bool
	maxsize    int
}

//一个dispatcher对应一个总的job队列
func NewDispatcher(maxworkernum, maxjobqueue int) (*Dispatcher, JobChan) {
	if maxworkernum <= 0 {
		maxworkernum = 100 //默认100
	}
	if maxjobqueue < maxworkernum {
		maxjobqueue = maxworkernum
	}
	thisJobQueue = make(JobChan, maxjobqueue)
	return &Dispatcher{
		WorkerPool: make(chan JobChan, maxworkernum),
		Workers:    make([]*worker, 0, maxworkernum),
		quit:       make(chan bool),
		maxsize:    maxworkernum,
	}, thisJobQueue
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxsize; i++ {
		worker := newWorker(d.WorkerPool)
		d.Workers = append(d.Workers, worker)
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-thisJobQueue:
			go func(job Job) {
				jobChan := <-d.WorkerPool
				jobChan <- job
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
