package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/naturali/kmr/config"
	"github.com/naturali/kmr/util/log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/clientcmd"
)

type PodDescription struct {
	Volumes      *[]map[string]interface{} `json:"volumes,omitempty" patchStrategy:"merge"`
	VolumeMounts *[]map[string]interface{} `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath"`
}

func CreateDockerImage(assetFolder, registry string, tags []string, includeFiles []string, workDir string) (string, error) {
	df := path.Join(assetFolder, "Dockerfile")
	dockerFileContent := `
	FROM alpine:3.5
	`
	for _, file := range includeFiles {
		rel, _ := filepath.Rel(assetFolder, file)
		dockerFileContent += fmt.Sprintf("COPY %v %v/\n", rel, workDir)
	}
	err := ioutil.WriteFile(df, []byte(dockerFileContent), 0666)
	if err != nil {
		return "", err
	}

	log.Info("Dockerfile is:\n", dockerFileContent)
	defer os.Remove(df)

	args := []string{"build", "-f", df}
	for i, tag := range tags {
		fn := registry + "/" + tag
		tags[i] = fn
		args = append(args, "-t", fn)
	}
	args = append(args, path.Dir(df))
	log.Info("Build command:", args)
	cmd := exec.Command("docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return "", err
	}
	log.Info("Pushing tags:")
	for _, tag := range tags {
		cmd := exec.Command("docker", "push", tag)
		cmd.Stdout = os.Stdout
		cmd.Stdin = os.Stdin
		cmd.Stderr = os.Stderr
		err = cmd.Run()
		if err != nil {
			return "", err
		}
	}
	return tags[0], nil
}

func CreateK8sKMRJob(jobName, serviceAccountName, namespace string, podDesc config.PodDescription,
	dockerImage, workDir string, command []string, port int32) (string, string, error) {
	var volumes []v1.Volume
	var volumeMounts []v1.VolumeMount

	if podDesc.Volumes != nil {
		jsonStr, _ := json.Marshal(podDesc.Volumes)
		json.Unmarshal(jsonStr, &volumes)
	}

	if podDesc.VolumeMounts != nil {
		jsonStr, _ := json.Marshal(podDesc.VolumeMounts)
		json.Unmarshal(jsonStr, &volumeMounts)
	}

	pod := v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
			Labels: map[string]string{
				"kmr.jobname": jobName,
				"kmr.master":  jobName + "-master",
				"app":         "kmr-master",
			},
		},
		Spec: v1.PodSpec{
			ServiceAccountName: serviceAccountName,
			RestartPolicy:      "Never",
			Containers: []v1.Container{
				{
					Name:  jobName + "-master",
					Image: dockerImage,
					Ports: []v1.ContainerPort{
						{
							ContainerPort: port,
						},
					},
					VolumeMounts: volumeMounts,
					WorkingDir:   workDir,
					Command:      command,
				},
			},
			Volumes: volumes,
		},
	}
	service := v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
			Labels: map[string]string{
				"kmr.jobname": jobName,
				"app":         "kmr-master",
			},
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceType("NodePort"),
			Ports: []v1.ServicePort{
				{
					Port:       port,
					TargetPort: intstr.FromInt(int(port)),
				},
			},
			Selector: map[string]string{
				"kmr.master": jobName + "-master",
			},
		},
	}
	k8sConfig, err := clientcmd.NewDefaultClientConfigLoadingRules().Load()
	if err != nil || k8sConfig == nil {
		return "", "", err
	}
	clientConfig := clientcmd.NewDefaultClientConfig(*k8sConfig, &clientcmd.ConfigOverrides{})
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return "", "", err
	}
	k8sclient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", "", err
	}
	falseVal := false
	k8sclient.ReplicaSets(namespace).Delete(jobName+"-mr", &metav1.DeleteOptions{
		OrphanDependents: &falseVal,
	})
	k8sclient.Pods(namespace).Delete(pod.Name, &metav1.DeleteOptions{})
	createdPod, err := k8sclient.Pods(namespace).Create(&pod)
	if err != nil {
		return "", "", err
	}
	k8sclient.Services(namespace).Delete(service.Name, &metav1.DeleteOptions{})
	createdService, err := k8sclient.Services(namespace).Create(&service)
	if err != nil {
		k8sclient.Pods(namespace).Delete(createdPod.Name, nil)
		return "", "", err
	}
	return createdPod.Name, createdService.Name, nil
}
