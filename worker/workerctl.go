package worker

type WorkerCtl interface {
	InspectWorker(workernum int) string
	StartWorkers(num int) error
	StopWorkers() error
	GetWorkerNum() int
}

// InspectWorkers Get information of workers
func InspectWorkers(w WorkerCtl, workerNums ...int) map[int]string {
	res := make(map[int]string)
	for _, wn := range workerNums {
		res[wn] = w.InspectWorker(wn)
	}
	return res
}
