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

	// Pre-populate the hash ring with existing StatefulSets.
	for _, ss := range ssList.Items {
		desired := int32(0)
		if ss.Spec.Replicas != nil {
			desired = *ss.Spec.Replicas
			log.Infof("Setting desired replicas for StatefulSet '%s' to %d", ss.Name, desired)
		}

		ready := ss.Status.ReadyReplicas

		// Only add fully ready StatefulSets (all replicas are ready) to the hash ring.
		// If a StatefulSet is partially ready, we rely on a future `Modified` event
		// to re-evaluate and add it to the hash ring once it becomes fully available.
		//
		// NOTE: In some cases, if the controller doesn’t emit a `Modified` event
		// for partially-ready StatefulSets reaching readiness, we may need a periodic
		// sync to ensure they eventually get added.
		if ready == desired {
			log.Infof("StatefulSet '%s' is fully ready (%d/%d replicas)\n", ss.Name, ready, desired)
			hashRing.Add(ss.Name)
		} else {
			log.Infof("StatefulSet '%s' is not fully ready (%d/%d replicas)\n", ss.Name, ready, desired)
			// skip adding statefulsets as partial readiness is not acceptable.
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

			// Once a StatefulSet becomes fully ready (all replicas available),
			// it may not trigger additional Modified events.
			// Ensure it's added to the hash ring as soon as it's fully ready.
			if ss.Status.ReadyReplicas == *ss.Spec.Replicas {
				log.Infof("All replicas are ready for StatefulSet: %s\n", ss.Name)
				hashRing.Add(ss.Name)
			}

		case watch.Modified:
			log.Infof("[MODIFIED] StatefulSet: %s | ReadyReplicas: %d/%d\n",
				ss.Name, ss.Status.ReadyReplicas, *ss.Spec.Replicas)

			// StatefulSets are only added to the hash ring when all replicas are ready.
			// However, we don't immediately remove them when one or more pods crash,
			// since the StatefulSet controller will typically reconcile and restore them.
			//
			// NOTE: During this interim period, write requests to the affected pod(s) may fail,
			// leading to temporary unavailability for certain keys.
			//
			// If quorum-based reads are used, this shouldn't affect overall availability,
			// unless a majority of the pods become unavailable at the same time.
			if ss.Status.ReadyReplicas == *ss.Spec.Replicas {
				log.Infof("All replicas are ready for StatefulSet: %s\n", ss.Name)
				hashRing.Add(ss.Name)
			}

		case watch.Deleted:
			log.Infof("[DELETED] StatefulSet: %s\n", ss.Name)

			// On complete deletion of a StatefulSet, ensure it's removed from the hash ring.
			// This prevents requests from being routed to pods that no longer exist.
			hashRing.Remove(ss.Name)

		case watch.Error:
			log.Infof("[ERROR] Watch error\n")
		}
	}

	fmt.Println("Watch channel closed")
}
