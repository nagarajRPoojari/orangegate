package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/nagarajRPoojari/orange/pkg/oql"
	"github.com/nagarajRPoojari/orangectl/internal/hash"
	"github.com/nagarajRPoojari/orangectl/internal/utils"
	log "github.com/nagarajRPoojari/orangectl/internal/utils/logger"
	"github.com/nagarajRPoojari/orangectl/internal/watch"
)

const (
	__K8S__REPLICA_COUNT__ = "__K8S__REPLICA_COUNT__"
	__K8S_NAMESAPCE__      = "__K8S_NAMESAPCE__"
)

var (
	targets []string
	mu      sync.RWMutex
)

// Proxy represents a network proxy that routes requests to backend replicas
// using consistent hashing via an OuterRing. It also manages client connections
// through a Cache and watches for changes using a Watcher.
type Proxy struct {
	// Addr is the network address of this proxy instance.
	Addr string

	// wt watches for updates or changes in the cluster (e.g., shards or replicas).
	wt watch.Watcher

	// hashRing is the consistent hashing ring used to route keys to shards.
	hashRing *hash.OuterRing

	// cache stores and reuses client connections to backend replicas.
	cache *Cache

	// k8s namespace within which pods are deployed, used to dynamically build
	// dns address of pods
	namespace string

	// replicaCount defines the number of replicas per shard managed by this proxy.
	replicaCount int
}

type Req struct {
	Query string `json:"query"`
}

func NewProxy(addr string) *Proxy {
	replicaCount := utils.GetEnv(__K8S__REPLICA_COUNT__, 0, true)
	namespcae := utils.GetEnv(__K8S_NAMESAPCE__, "", true)

	t := &Proxy{
		Addr: addr,
		wt:   *watch.NewWatcher(),
		// keeping 3 virtual nodes per shard to ensure uniform distribution
		hashRing:     hash.NewHashRing(3, replicaCount, namespcae),
		namespace:    namespcae,
		replicaCount: replicaCount,
		cache:        NewCache(),
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()

		var req Req

		err := json.NewDecoder(r.Body).Decode(&req)

		res, err := t.processQuery(req.Query)
		log.Infof("recevied result, res=%v, err=%v, query: %v", res, err, req.Query)
		if err != nil {
			http.Error(w, "Proxy error: "+err.Error(), 502)
			return
		}

		if res == nil {
			res = "sucess"
			return
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

	return t
}

func (t *Proxy) Serve() {
	http.ListenAndServe(t.Addr, nil)
}

func (t *Proxy) WatchShards() {
	go t.wt.Run(t.hashRing)
}

// buildAddr constructs the full DNS address for a pod within a shard in the given namespace.
// The format follows Kubernetes DNS conventions for service discovery.
func buildAddr(shard string, podId int, namespace string) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", shard, podId, shard, namespace)
}

// processQuery parses and executes an OQL query string by dispatching it to the appropriate
// operation handler. It supports Create, Insert, Select, and Delete operations, routing
// requests to the correct shards and replicas via the consistent hashing ring and cached clients.
//
// Returns the operation result or an error if the query is invalid or the operation fails.
func (t *Proxy) processQuery(q string) (any, error) {
	parser := oql.NewParser(q)
	op, err := parser.Build()
	if err != nil {
		return nil, err
	}

	switch v := op.(type) {
	case oql.CreateOp:
		// create requests are broadcasted to all nodes of all shards
		shs := t.hashRing.GetAllShards()
		for _, sh := range shs {
			for i := range t.replicaCount {
				go func(i int) {
					log.Infof("calling create to: ", buildAddr(sh.Id, i, t.namespace))
					cl := t.cache.Get(buildAddr(sh.Id, i, t.namespace))
					cl.Create(&v)
				}(i)
			}
		}
		return nil, nil

	case oql.InsertOp:
		var key int64
		switch val := v.Value["_ID"].(type) {
		case int64:
			key = val
		case int:
			key = int64(val)
		case float64:
			key = int64(val)
		case json.Number:
			k, err := val.Int64()
			if err == nil {
				key = k
			} else {
				log.Fatalf("can't cast json val, %v", val)
			}
		default:
			log.Fatalf("can't cast, %v", val)
		}
		addr := t.hashRing.GetNode(fmt.Sprint(key))
		log.Infof("calling insert to: %s", addr)
		cl := t.cache.Get(addr)
		return nil, cl.Insert(&v)

	case oql.SelectOp:
		key := v.ID
		sh := t.hashRing.GetShard(fmt.Sprint(key))
		var res []byte
		var err error
		for i := range t.replicaCount {
			cl := t.cache.Get(buildAddr(sh.Id, i, t.namespace))
			// offcourse this is not how quorum works
			res, err = cl.Select(&v)
		}
		return res, err

	case oql.DeleteOp:
		return nil, fmt.Errorf("delete op not implpemented")
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}
