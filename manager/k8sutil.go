package manager

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/pkg/api/v1"
)

var (
	masterPort int32 = 50051
)

// NewMasterPod NewMasterPod
func (server *KmrManagerWeb) NewMasterPod(name, image string, command []string) *v1.Pod {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kmr.jobname": name,
				"app":         "kmr-master",
			},
		},
		Spec: v1.PodSpec{
			ServiceAccountName: "kmr-master",
			RestartPolicy:      v1.RestartPolicyNever,
			Containers: []v1.Container{
				v1.Container{
					Name:    "kmr-master",
					Image:   image,
					Command: command,
					Ports: []v1.ContainerPort{
						v1.ContainerPort{
							ContainerPort: masterPort,
						},
					},
				},
			},
		},
	}

	return &pod
}

// NewMasterService NewMasterService
func (server *KmrManagerWeb) NewMasterService(name string) *v1.Service {
	service := v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kmr.jobname": name,
				"app":         "kmr-master",
			},
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				v1.ServicePort{
					Port:       masterPort,
					TargetPort: intstr.FromInt(int(masterPort)),
				},
			},
		},
	}
	return &service
}

func (server *KmrManagerWeb) StartMaster(pod *v1.Pod, service *v1.Service) error {
	var err error
	_, err = server.k8sclient.Services(server.Namespace).Create(service)
	if err != nil {
		return err
	}
	_, err = server.k8sclient.Pods(server.Namespace).Create(pod)
	if err != nil {
		return err
	}
	return nil
}
