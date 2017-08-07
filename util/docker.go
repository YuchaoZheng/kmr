package util

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"encoding/json"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"fmt"
	"io/ioutil"
	"os"
	"github.com/naturali/kmr/util/log"
	"path"
	"context"
	"io"
	"github.com/docker/docker/builder"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/docker/docker/pkg/progress"
	"github.com/docker/docker/client"
	"github.com/docker/docker/api/types"
)

type PodDescription struct {
	Volumes      *[]map[string]interface{} `json:"volumes,omitempty" patchStrategy:"merge"`
	VolumeMounts *[]map[string]interface{} `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath"`
}


func CreateDockerImage(registry string, tags []string, includeFiles []string, dir string) (string, error) {
	df := "./Dockerfile"
	dockerFileContent := `
	FROM alpine:3.5
	`
	for _, file := range includeFiles {
		dockerFileContent += fmt.Sprintf("COPY %v %v\n", file, dir)
	}
	err := ioutil.WriteFile(df, []byte(dockerFileContent), 0666)
	if err != nil {
		return "", err
	}

	log.Info("Dockerfile is:\n", dockerFileContent)
	defer os.Remove(df)

	dockerCli, err := client.NewEnvClient()
	ctx := context.Background()
	if err != nil {
		return "", err
	}
	contextDir, _, err := builder.GetContextFromLocalDir(path.Dir(df), path.Base(df))
	if err != nil {
		return "", err
	}
	if err := builder.ValidateContextDirectory(contextDir, nil); err != nil {
		return "", err
	}
	buildCtx, err := archive.TarWithOptions(contextDir, &archive.TarOptions{
		Compression:     archive.Uncompressed,
		IncludeFiles:    includeFiles,
	})
	progressOutput := streamformatter.NewStreamFormatter().NewProgressOutput(os.Stdout, true)
	var body io.Reader = progress.NewProgressReader(buildCtx, progressOutput, 0, "", "Sending build context to Docker daemon")
	for i, tag := range tags {
		fn := registry + "/" + tag
		tags[i] = fn
	}
	_, err = dockerCli.ImageBuild(ctx, body, types.ImageBuildOptions{
		Dockerfile: df,
		Tags: tags,
	})

	if err != nil {
		return "", err
	}


	if len(tags) != 0 {
		log.Info("Pushing tags:")
		for _, tag := range tags {
			dockerCli.ImagePush(ctx, tag, types.ImagePushOptions{})
		}
		return tags[0], nil
	} else {
		return "", nil
	}
}

func CreateK8sKMRJob(jobName , serviceAccountName , namespace string, podDesc PodDescription,
	dockerImage , workDir string, command []string, port int32) (string, string, error) {
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

	pod := v1.Pod {
		TypeMeta:metav1.TypeMeta{
			Kind: "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
			Labels: map[string]string{
				"name": jobName,
			},
		},
		Spec: v1.PodSpec{
			ServiceAccountName: serviceAccountName,
			RestartPolicy: "Never",
			Containers: []v1.Container {
				{
					Name: jobName + "-master",
					Image: dockerImage,
					Ports: []v1.ContainerPort{
						{
							ContainerPort: port,
						},
					},
					VolumeMounts: volumeMounts,
					WorkingDir: workDir,
					Command: command,
				},
			},
			Volumes: volumes,
		},
	}
	service := v1.Service {
		TypeMeta:metav1.TypeMeta{
			Kind: "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
			Labels: map[string]string{
				"app": jobName,
			},
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceType("NodePort"),
			Ports: []v1.ServicePort{
				{
					Port: port,
					TargetPort: intstr.FromInt(int(port)),
				},
			},
			Selector: map[string]string {
				"name": jobName,
			},
		},
	}
	k8sConfig, err := clientcmd.NewDefaultClientConfigLoadingRules().Load()
	if err == nil || k8sConfig == nil{
		return "", "", err
	}
	clientConfig := clientcmd.NewDefaultClientConfig(*k8sConfig, nil)
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return "", "", err
	}
	k8sclient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", "", err
	}
	createdPod, err := k8sclient.Pods(namespace).Update(&pod)
	if err != nil {
		return "", "", err
	}
	createdService, err := k8sclient.Services(namespace).Update(&service)
	if err != nil {
		k8sclient.Pods(namespace).Delete(createdPod.Name, nil)
		return "", "", err
	}
	return createdPod.Name, createdService.Name, nil
}
