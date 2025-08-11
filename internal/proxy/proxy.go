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
	__ACK_LEVEL__          = "__ACK_LEVEL__"

	// values
	__QUORUM__ = "quorum"
	__ALL__    = "all"
	__SINGLE__ = "single"
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

	// ack level
	ackLevel string
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
		// by default setting ack level to quorum
		ackLevel: utils.GetEnv(__ACK_LEVEL__, __QUORUM__),
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Acquire read lock for thread-safe access
		mu.RLock()
		defer mu.RUnlock()

		var req Req

		// Parse and validate the request body
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Errorf("Failed to decode request body: %v", err)
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		// Process the query using the target logic
		res, err := t.processQuery(req.Query)
		if err != nil {
			log.Errorf("Error processing query [%s]: %v", req.Query, err)
			http.Error(w, "Failed to process query: "+err.Error(), http.StatusBadGateway)
			return
		}

		log.Infof("Processed query successfully: %s", req.Query)

		// Handle nil response gracefully
		if res == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`"success"`))
			return
		}

		// Ensure the response is in the expected []byte format
		resBytes, ok := res.([]byte)
		if !ok {
			log.Errorf("Unexpected response type for query [%s]: %T", req.Query, res)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Attempt to unmarshal the response
		var data interface{}
		if err := json.Unmarshal(resBytes, &data); err != nil {
			log.Errorf("Failed to unmarshal response for query [%s]: %v", req.Query, err)
			http.Error(w, "Failed to decode backend response", http.StatusBadGateway)
			return
		}

		// Marshal the result into pretty-printed JSON
		formattedJSON, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			log.Errorf("Failed to format response for query [%s]: %v", req.Query, err)
			http.Error(w, "Failed to format response", http.StatusInternalServerError)
			return
		}

		// Return the formatted JSON response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(formattedJSON)
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
		// create requests are broadcasted to all nodes of all shards.
		// @todo: need complete ack from all nodes before acknowledging
		// back to client
		shs := t.hashRing.GetAllShards()
		for _, sh := range shs {
			for i := range t.replicaCount {
				log.Infof("calling create to: %s", buildAddr(sh.Id, i, t.namespace))
				cl := t.cache.Get(buildAddr(sh.Id, i, t.namespace))
				// If connection is already lost, gRPC tries to reconnect with a timeout.
				// @issue: if pod is down & cannot spin up within timeout it will fail to
				// process create request leading to all further insert failures.
				if err := cl.Create(&v); err != nil {
					log.Errorf("error while creating: %v ", err)
				}
			}
		}
		return nil, nil

	case oql.InsertOp:
		var key int64
		// OQL allows user to write _ID in any form that can be converted to int64
		switch val := v.Value["_ID"].(type) {
		case int64:
			key = val
		case int:
			key = int64(val)
		case float64:
			key = int64(val)
		// allowing json.Number for further support json types
		case json.Number:
			k, err := val.Int64()
			if err != nil {
				return nil, fmt.Errorf("can't cast json val, %v", val)
			}
			key = k
		default:
			return nil, fmt.Errorf("can't cast, %v", val)
		}

		addr := t.hashRing.GetNode(fmt.Sprint(key))
		log.Infof("calling insert to: %s", addr)

		cl := t.cache.Get(addr)
		// If connection is already lost, gRPC tries to reconnect with a timeout.
		// @issue: if pod is down & cannot spin up within timeout it will fail to
		// process create request leading to all further insert failures.
		return nil, cl.Insert(&v)

	case oql.SelectOp:
		key := v.ID
		sh := t.hashRing.GetShard(fmt.Sprint(key))

		var result any
		var w int // Number of successful responses required (write quorum)

		// Determine required acknowledgment level based on consistency config
		switch t.ackLevel {
		case __QUORUM__:
			// majority of replicas
			w = t.replicaCount/2 + 1
		case __ALL__:
			// all replicas must respond
			w = t.replicaCount
		case __SINGLE__:
			// accept first successful response (eventual consistency)
			w = 0
		}

		var wg sync.WaitGroup
		wg.Add(w)

		// Launch concurrent requests to all replicas
		for i := range t.replicaCount {
			go func(replicaIndex int) {
				defer func() {
					if r := recover(); r != nil {
						log.Errorf("Recovered from panic in replica %d: %v", replicaIndex, r)
					}
				}()

				addr := buildAddr(sh.Id, replicaIndex, t.namespace)
				cl := t.cache.Get(addr)

				if res, err := cl.Select(&v); err == nil {
					// Store first successful result
					result = res
					wg.Done()
				} else {
					log.Warnf("Select failed on replica %d: %v", replicaIndex, err)
				}
			}(i)
		}

		// wait until required number of replicas have responded successfully
		wg.Wait()
		return result, err

	case oql.DeleteOp:
		return nil, fmt.Errorf("delete op not implpemented")
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}
