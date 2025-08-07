package watch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nagarajRPoojari/orangegate/internal/hash"
	"github.com/nagarajRPoojari/orangegate/internal/utils"
	log "github.com/nagarajRPoojari/orangegate/internal/utils/logger"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	__K8S_NAMESAPCE__    = "__K8S_NAMESAPCE__"
	__K8S_POD_SELECTOR__ = "__K8S_POD_SELECTOR__"
	__BUILD_MODE__       = "__BUILD_MODE__"

	__DEV__  = "dev"
	__PROD__ = "prod"
)

type Watcher struct {
}

func NewWatcher() *Watcher {
	return &Watcher{}
}

func (t *Watcher) Run(hashRing *hash.HashRing) {
	// Use in-cluster config
	mode := utils.GetEnv(__BUILD_MODE__, __DEV__)
	var config *rest.Config
	var err error
	if mode == __DEV__ {
		config, err = clientcmd.BuildConfigFromFlags("", filepath.Join(os.Getenv("HOME"), ".kube", "config"))
		if err != nil {
			log.Fatalf("failed to extract kube config, err=%v", err)
		}
	}
	config, err = rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Failed to load in-cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create clientset: %v", err))
	}

	namespace := utils.GetEnv(__K8S_NAMESAPCE__, "", true)
	labelVal := utils.GetEnv(__K8S_POD_SELECTOR__, "", true)
	labelSelector := fmt.Sprintf("pod-selector=%s", labelVal)

	podList, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		log.Fatalf("Error listing pods: %v", err)
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase == v1.PodRunning {
			log.Infof("[EXISTING] Pod %s is already running with IP %s", pod.Name, pod.Status.PodIP)
			hashRing.Add(pod.Status.PodIP, 52001)
		}
	}

	watcher, err := clientset.CoreV1().Pods(namespace).Watch(context.TODO(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		log.Fatalf("Failed to start pod watcher: %v", err)
	}
	defer watcher.Stop()

	log.Infof("Watching pods in namespace '%s' with label selector '%s'\n", namespace, labelSelector)

	for event := range watcher.ResultChan() {
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			log.Warnf("Unexpected type")
			continue
		}

		switch event.Type {
		case watch.Added:
			log.Infof("[ADDED] Pod %s - Phase: %s\n", pod.Name, pod.Status.Phase)
		case watch.Modified:
			log.Infof("[MODIFIED] Pod %s - Phase: %s\n", pod.Name, pod.Status.Phase)
			if pod.Status.Phase == "Running" {
				hashRing.Add(pod.Status.PodIP, 52001)
			}
		case watch.Deleted:
			log.Infof("[DELETED] Pod %s\n", pod.Name)
			hashRing.Remove(pod.Status.PodIP)
		default:
			log.Infof("[OTHER] Pod %s - Event: %s\n", pod.Name, event.Type)
		}
	}
}
