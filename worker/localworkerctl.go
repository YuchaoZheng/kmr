package worker

import (
	"math/rand"
	"strconv"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/jobgraph"
)

type LocalWorkerCtl struct {
	j            *jobgraph.Job
	buckets      []bucket.Bucket
	flushOutSize int
	port         int

	workers []*executor.Worker
}

func (w *LocalWorkerCtl) InspectWorker(workernum int) string {
	return ""
}
func (w *LocalWorkerCtl) StartWorkers(num int) error {
	for i := 0; i < num; i++ {
		workerID := rand.Int63()
		ew := executor.NewWorker(w.j, workerID, "localhost:"+strconv.Itoa(w.port), w.flushOutSize, w.buckets[0], w.buckets[1], w.buckets[2])
		w.workers = append(w.workers, ew)
		go ew.Run()
	}
	return nil
}
func (w *LocalWorkerCtl) StopWorkers() error {
	return nil
}

func (w *LocalWorkerCtl) GetWorkerNum() int {
	return len(w.workers)
}

func NewLocalWorkerCtl(j *jobgraph.Job, port int, flushOutSize int, buckets []bucket.Bucket) *LocalWorkerCtl {
	res := &LocalWorkerCtl{
		j,
		buckets,
		flushOutSize,
		port,
		make([]*executor.Worker, 0),
	}
	return res
}
