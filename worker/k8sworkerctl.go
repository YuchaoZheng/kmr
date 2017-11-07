package worker

import (
	"encoding/json"
	"fmt"
	"log"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	v1beta1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/rest"
)

type K8sWorkerConfig struct {
	Name         string
	CPULimit     string
	BinaryPath   string
	Image        string
	Volumes      []map[string]interface{} `json:"volumes,omitempty" patchStrategy:"merge"`
	VolumeMounts []map[string]interface{} `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath"`
	Namespace    string
	K8sConfig    rest.Config
	WorkerNum    int
	RandomSeed   int64
	Command      []string
}

type K8sWorkerCtl struct {
	config    K8sWorkerConfig
	k8sclient *kubernetes.Clientset
}

func (w *K8sWorkerCtl) newReplicaSet(name string, command []string, image string, replicas int32) v1beta1.ReplicaSet {
	// generate resourceRequirements
	var resourceRequirements v1.ResourceRequirements
	if w.config.CPULimit != "" {
		cpulimt, err := resource.ParseQuantity(w.config.CPULimit)
		if err != nil {
			log.Fatalf("Can't parse cpulimit \"%s\": %v", w.config.CPULimit, err)
		}
		resourceRequirements = v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu": cpulimt,
			},
			Limits: v1.ResourceList{
				"cpu": cpulimt,
			},
		}
	}
	// generate volume stuff
	var volumes []v1.Volume
	var volumeMounts []v1.VolumeMount

	if len(w.config.Volumes) > 0 {
		jsonStr, _ := json.Marshal(w.config.Volumes)
		json.Unmarshal(jsonStr, &volumes)
	}

	if len(w.config.VolumeMounts) > 0 {
		jsonStr, _ := json.Marshal(w.config.VolumeMounts)
		json.Unmarshal(jsonStr, &volumeMounts)
	}

	// pod
	podTemplate := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kmr.jobname": name,
				"app":         "kmr-worker",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				v1.Container{
					Name:    "kmr-worker",
					Command: command,
					Image:   image,
					Env: []v1.EnvVar{
						v1.EnvVar{
							Name:  "KMR_MASTER_ADDRESS",
							Value: fmt.Sprintf("%s%s", w.config.Name, "50051"),
						},
					},
					VolumeMounts: volumeMounts,
					Resources:    resourceRequirements,
				},
			},
			Volumes: volumes,
		},
	}
	rsSpec := v1beta1.ReplicaSetSpec{
		Replicas: &replicas,
		Template: podTemplate,
	}
	replicaSet := v1beta1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kmr.jobname": name,
				"app":         "kmr",
			},
		},
		Spec: rsSpec,
	}
	return replicaSet
}

func (w *K8sWorkerCtl) createReplicaSet(replicaSet *v1beta1.ReplicaSet) (*v1beta1.ReplicaSet, error) {
	return w.k8sclient.ExtensionsV1beta1().
		ReplicaSets(w.config.Namespace).Create(replicaSet)
}

func (w *K8sWorkerCtl) replicaSetName(jobName string) string {
	return fmt.Sprintf("%s", jobName)
}

func (w *K8sWorkerCtl) InspectWorker(workernum int) string {
	return ""
}

func (w *K8sWorkerCtl) StartWorkers(num int) error {
	var rs v1beta1.ReplicaSet
	rs = w.newReplicaSet(w.replicaSetName(w.config.Name),
		w.config.Command, w.config.Image, int32(w.config.WorkerNum))
	_, err := w.createReplicaSet(&rs)
	return err
}
func (w *K8sWorkerCtl) StopWorkers() error {
	falseVal := false
	return w.k8sclient.ExtensionsV1beta1().ReplicaSets(w.config.Namespace).
		Delete(w.replicaSetName(w.config.Name), &metav1.DeleteOptions{
			OrphanDependents: &falseVal,
		})
}
func (w *K8sWorkerCtl) GetWorkerNum() int {
	return w.config.WorkerNum
}

func NewK8sWorkerCtl(workerConfig *K8sWorkerConfig) *K8sWorkerCtl {
	res := &K8sWorkerCtl{}
	res.config = *workerConfig
	var err error
	res.k8sclient, err = kubernetes.NewForConfig(&workerConfig.K8sConfig)
	if err != nil {
		log.Fatalf("Can't init kubernetes client , %v", err)
	}
	return res
}
