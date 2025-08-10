package watch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nagarajRPoojari/orangectl/internal/hash"
	"github.com/nagarajRPoojari/orangectl/internal/utils"
	log "github.com/nagarajRPoojari/orangectl/internal/utils/logger"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	__K8S_NAMESAPCE__      = "__K8S_NAMESAPCE__"
	__K8S_SHARD_SELECTOR__ = "__K8S_SHARD_SELECTOR__"
	__BUILD_MODE__         = "__BUILD_MODE__"

	__DEV__  = "dev"
	__PROD__ = "prod"
)

type Watcher struct {
}

func NewWatcher() *Watcher {
	return &Watcher{}
}

// Run starts the watcher that monitors Kubernetes StatefulSets with a specific label selector
// in the configured namespace. It initializes the Kubernetes client configuration, lists the
// existing StatefulSets to populate the consistent hash ring, and then watches for add, modify,
// and delete events to update the hash ring dynamically.
//
// The hashRing parameter is updated in response to StatefulSet lifecycle events, adding or removing
// shards as StatefulSets become fully ready or are deleted.
//
// This function blocks while watching events and logs relevant state changes.
func (t *Watcher) Run(hashRing *hash.OuterRing) {
	// Use in-cluster config
	mode := utils.GetEnv(__BUILD_MODE__, __DEV__)
	var config *rest.Config
	var err error
	// build the config from local k8s config file when running outside k8s (in DEV mode)
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
	labelVal := utils.GetEnv(__K8S_SHARD_SELECTOR__, "", true)
	labelSelector := fmt.Sprintf("shard-selector=%s", labelVal)

	ssList, err := clientset.AppsV1().StatefulSets(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		log.Fatalf("Error listing StatefulSets: %v", err)
	}

	// pre-populating hash ring with existing statefulsets
	for _, ss := range ssList.Items {
		desired := int32(0)
		if ss.Spec.Replicas != nil {
			desired = *ss.Spec.Replicas
			log.Infof("setting desired replicas to %d", desired)
		}

		ready := ss.Status.ReadyReplicas
		// adding only fully up statefulsets with all replicas
		// half up (one with fewer ready replicas) will eventually result
		// in `modified` event
		// @address: not sure will it result in modified event or not
		if ready == desired {
			log.Infof("StatefulSet '%s' is fully ready (%d/%d replicas)\n", ss.Name, ready, desired)
			hashRing.Add(ss.Name)
		} else {
			log.Infof("StatefulSet '%s' is not ready (%d/%d replicas)\n", ss.Name, ready, desired)
			hashRing.Add(ss.Name)
		}
	}

	watcher, err := clientset.AppsV1().StatefulSets(namespace).Watch(context.TODO(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		log.Fatalf("Failed to start statefulset watcher: %v", err)
	}
	defer watcher.Stop()

	log.Infof("Watching statefulsets in namespace '%s' with label selector '%s'\n", namespace, labelSelector)

	ch := watcher.ResultChan()
	for event := range ch {
		ss, ok := event.Object.(*appsv1.StatefulSet)
		if !ok {
			fmt.Println("unexpected type")
			continue
		}

		switch event.Type {
		case watch.Added:
			log.Infof("[ADDED] StatefulSet: %s\n", ss.Name)
			if ss.Status.ReadyReplicas == *ss.Spec.Replicas {
				log.Infof("All replicas are ready for StatefulSet: %s\n", ss.Name)
				hashRing.Add(ss.Name)
			}
		case watch.Modified:
			log.Infof("[MODIFIED] StatefulSet: %s | ReadyReplicas: %d/%d\n",
				ss.Name, ss.Status.ReadyReplicas, *ss.Spec.Replicas)

			// statefulsets are only added when it is up with all replicas but
			// we are also not removing as soon one or more pod crashes since it
			// will eventually spinned up by StatefulSet reconciler itself.
			// @issue: by that time write requests to that node will fail
			//		  resulting in a temperory down for few set of keys
			//		  if we are using quorum reads it shouldn't affecy unless majority of the
			//		  pods are down simultaniously
			if ss.Status.ReadyReplicas == *ss.Spec.Replicas {
				log.Infof("All replicas are ready for StatefulSet: %s\n", ss.Name)
				hashRing.Add(ss.Name)
			}

		case watch.Deleted:
			log.Infof("[DELETED] StatefulSet: %s\n", ss.Name)
			hashRing.Remove(ss.Name)

		case watch.Error:
			log.Infof("[ERROR] Watch error\n")
		}
	}

	fmt.Println("Watch channel closed")
}
