package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/naturali/kmr/manager"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	port        = flag.String("port", ":50081", "port")
	namespace   = flag.String("namespace", "kmr", "kubernetes namespace to run KMR task")
	configStore = flag.String("config-store", "/tmp", "path to save job config")
)

func main() {
	var err error

	// K8s Client
	var config *rest.Config
	var clientset *kubernetes.Clientset

	k8sSchema := os.Getenv("KUBERNETES_API_SCHEMA")
	if k8sSchema != "http" { // InCluster
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatalf("Can't get incluster config, %v", err)
		}
	} else { // For debug usage. > source dev_environment.sh
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT")

		config = &rest.Config{
			Host: fmt.Sprintf("%s://%s", k8sSchema, net.JoinHostPort(host, port)),
		}
		token := os.Getenv("KUBERNETES_API_ACCOUNT_TOKEN")
		if len(token) > 0 {
			config.BearerToken = token
		}
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Can't init kubernetes client , %v", err)
	}

	// server
	server := manager.NewKmrManagerWeb(*namespace, *configStore, clientset)
	server.Serve(port)

}
