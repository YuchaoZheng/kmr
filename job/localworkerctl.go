package job

type LocalWorkerCtl struct {
	j *JobGraph
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
		}
		go w.runWorker()
	}
	return nil
}
func (w *LocalWorkerCtl) StopWorkers() {

}
func (w *LocalWorkerCtl) GetWorkerNum() int {
	return 1
}

func NewLocalWorkerCtl(j *JobGraph) *LocalWorkerCtl {
	res := &LocalWorkerCtl{
		j,
	}
	return res
}
