package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/nagarajRPoojari/orange/net/client"
	"github.com/nagarajRPoojari/orange/pkg/query"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	targets []string
	mu      sync.RWMutex
)

const configPath = "/app/config/SERVER_ADDRESS"

type Req struct {
	Query string `json:"query"`
}

func main() {

	// Initial load
	loadTargets()

	// Start file watcher
	go watchConfig()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()

		if len(targets) == 0 {
			http.Error(w, "No targets available", http.StatusServiceUnavailable)
			return
		}

		target := targets[0]
		fullURL := mustParse(target)

		fmt.Println(fullURL)

		var req Req

		err := json.NewDecoder(r.Body).Decode(&req)

		cl := client.NewClient(fullURL.String(), 52001)
		res, err := processQuery(cl, req.Query)

		if err != nil {
			http.Error(w, "Proxy error: "+err.Error(), 502)
			return
		}

		if res == nil {
			res = "sucess"
		}

		var data interface{}
		resBytes := res.([]byte)
		if err := json.Unmarshal(resBytes, &data); err != nil {
			http.Error(w, "Failed to unmarshal: "+err.Error(), 502)
			return
		}

		response, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			http.Error(w, "Failed to format: "+err.Error(), 502)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(response)
	})

	log.Println("Proxy up on :8000")
	http.ListenAndServe(":8000", nil)
}

func mustParse(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		log.Println("Invalid URL:", s)
	}
	return u
}

func loadTargets() {
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Println("Failed to read config:", err)
		return
	}
	var newTargets []string
	if err := json.Unmarshal(data, &newTargets); err != nil {
		log.Println("Bad config format:", err)
		return
	}

	mu.Lock()
	targets = newTargets
	mu.Unlock()
	log.Println("Reloaded targets:", targets)
}

func watchConfig() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("Failed to set up watcher:", err)
	}
	defer watcher.Close()

	configDir := "/app/config"
	if err := watcher.Add(configDir); err != nil {
		log.Fatal("Failed to watch directory:", err)
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			fmt.Println(event)
			if !ok {
				return
			}
			if event.Name == configPath &&
				(event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename)) != 0 {
				log.Println("Config change detected:", event)
				loadTargets()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Println("Watcher error:", err)
		}
	}
}

func processQuery(client *client.Client, q string) (any, error) {
	parser := query.NewParser(q)
	op, err := parser.Build()
	if err != nil {
		return nil, err
	}

	switch v := op.(type) {
	case query.CreateOp:
		return nil, client.Create(&v)
	case query.InsertOp:
		return nil, client.Insert(&v)
	case query.SelectOp:
		return client.Select(&v)
	case query.DeleteOp:
		return nil, fmt.Errorf("delete op not implpemented")
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}

func watchShards() {
	// Use in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load in-cluster config: %v", err))
	}

	// Create clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create clientset: %v", err))
	}

	namespace := "default"       // Change if needed
	labelSelector := "app=myapp" // Customize this to match your pods

	// Start watching Pods
	watcher, err := clientset.CoreV1().Pods(namespace).Watch(context.TODO(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to start pod watcher: %v", err))
	}
	defer watcher.Stop()

	fmt.Printf("Watching pods in namespace '%s' with label selector '%s'\n", namespace, labelSelector)

	// Handle events
	for event := range watcher.ResultChan() {
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			fmt.Println("Unexpected type")
			continue
		}

		switch event.Type {
		case watch.Added:
			fmt.Printf("[ADDED] Pod %s - Phase: %s\n", pod.Name, pod.Status.Phase)
		case watch.Modified:
			fmt.Printf("[MODIFIED] Pod %s - Phase: %s\n", pod.Name, pod.Status.Phase)
		case watch.Deleted:
			fmt.Printf("[DELETED] Pod %s\n", pod.Name)
		default:
			fmt.Printf("[OTHER] Pod %s - Event: %s\n", pod.Name, event.Type)
		}
	}
}
