package master

import (
	"encoding/json"
	"fmt"
	"log"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/pkg/api/v1"
	v1beta1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

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

func (w *K8sWorkerCtl) replicaSetName(phase string, jobName string) string {
	return fmt.Sprintf("%s-%s", jobName, phase)
}

func (w *K8sWorkerCtl) startWorker(phase string) error {
	// var rs v1beta1.ReplicaSet
	// switch phase {
	// case mapPhase:
	// 	rs = w.newReplicaSet(w.replicaSetName(phase, w.Name),
	// 		master.JobDesc.Map.Command, master.JobDesc.Map.Image, int32(master.JobDesc.Map.NWorker))
	// case reducePhase:
	// 	rs = w.newReplicaSet(master.replicaSetName(phase, master.JobName),
	// 		master.JobDesc.Reduce.Command, master.JobDesc.Reduce.Image, int32(master.JobDesc.Reduce.NWorker))
	// case mapreducePhase:
	// 	rs = master.newReplicaSet(master.replicaSetName(phase, master.JobName),
	// 		master.JobDesc.Command, master.JobDesc.Image, int32(master.JobDesc.NWorker))
	// }
	// _, err := master.createReplicaSet(&rs)
	return nil
}
func (w *K8sWorkerCtl) killWorkers(phase string) error {

	return nil
	// return master.k8sclient.ExtensionsV1beta1().ReplicaSets(master.namespace).
	// 	Delete(master.replicaSetName(phase, master.JobName), &metav1.DeleteOptions{
	// 		OrphanDependents: &falseVal,
	// 	})
}
