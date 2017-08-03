package master

import "k8s.io/client-go/rest"
import "k8s.io/client-go/kubernetes"

type K8sWorkerConfig struct {
	Name         string
	CPULimit     string
	Command      string
	Image        string
	Volumes      []map[string]interface{} `json:"volumes,omitempty" patchStrategy:"merge"`
	VolumeMounts []map[string]interface{} `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath"`
	Namespace    string
	K8sConfig    rest.Config
}

type K8sWorkerCtl struct {
	config    K8sWorkerConfig
	k8sclient *kubernetes.Clientset
}

func (w *K8sWorkerCtl) InspectWorker(workernum int) string {
	return ""
}
func (w *K8sWorkerCtl) StartWorkers(num int) error {
	return nil
}
func (w *K8sWorkerCtl) StopWorkers() {

}
func (w *K8sWorkerCtl) GetWorkerNum() int {
	return 1
}

func NewK8sWorkerCtl() *K8sWorkerCtl {
	res := &K8sWorkerCtl{}
	res.k8sclient, _ = kubernetes.NewForConfig(&res.config.K8sConfig)
	return res
}
