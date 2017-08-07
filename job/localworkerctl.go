package job

import "strconv"

type LocalWorkerCtl struct {
	j *JobGraph
	port int
}

func (w *LocalWorkerCtl) InspectWorker(workernum int) string {
	return ""
}
func (w *LocalWorkerCtl) StartWorkers(num int) error {
	for i := 0; i < num ;i ++ {
		w := worker{
			w.j,
			workerIDs[i],
			50,
			"localhost:" + strconv.Itoa(w.port),
		}
		go w.runWorker()
	}
	return nil
}
func (w *LocalWorkerCtl) StopWorkers() error {
	return nil
}

func (w *LocalWorkerCtl) GetWorkerNum() int {
	return len(workerIDs)
}

func NewLocalWorkerCtl(j *JobGraph, port int) *LocalWorkerCtl {
	res := &LocalWorkerCtl{
		j,
		port,
	}
	return res
}
