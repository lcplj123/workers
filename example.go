package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lcplj123/workers"
)

//只要MyJob实现Do接口即可
type MyJob struct {
	t int
}

func NewMyJob(t int) *MyJob {
	return &MyJob{
		t: t,
	}
}

func (m *MyJob) Do() error {
	time.Sleep(time.Duration(m.t) * time.Millisecond)
	fmt.Println("job --- t = ", m.t)
	return nil
}

var count int64 = 0

func main() {
	d, JobQueue := workers.NewDispatcher(10, 200)
	d.Run()

	r := gin.Default()
	r.GET("/test", func(c *gin.Context) {
		count++
		t := time.Now()
		rand.Seed(t.UnixNano())
		for i := 0; i < 200; i++ {
			b := rand.Intn(10000)
			w := NewMyJob(b)
			JobQueue <- w
		}
		c.String(http.StatusOK, strconv.FormatInt(count, 10))
		return
	})

	r.GET("/stat", func(c *gin.Context) {
		waitJob, doneJob, DoneJob := d.Stat()
		s := fmt.Sprintf("waitjob=%d, doneJob=%d,DoneJob=%d", waitJob, doneJob, DoneJob)
		c.String(http.StatusOK, s)
		return
	})

	r.Run(":80")
}
