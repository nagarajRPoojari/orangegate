package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/nagarajRPoojari/orange/pkg/oql"
	"github.com/nagarajRPoojari/orangegate/internal/hash"
	log "github.com/nagarajRPoojari/orangegate/internal/utils/logger"

	"github.com/nagarajRPoojari/orangegate/internal/watch"
)

var (
	targets []string
	mu      sync.RWMutex
)

type Proxy struct {
	Addr     string
	wt       watch.Watcher
	hashRing *hash.HashRing
}

type Req struct {
	Query string `json:"query"`
}

func NewProxy(addr string) *Proxy {
	t := &Proxy{Addr: addr, wt: *watch.NewWatcher(), hashRing: hash.NewHashRing(3)}
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

func (t *Proxy) processQuery(q string) (any, error) {
	parser := oql.NewParser(q)
	op, err := parser.Build()
	if err != nil {
		return nil, err
	}

	switch v := op.(type) {
	case oql.CreateOp:
		for _, cl := range t.hashRing.GetAll() {
			cl.Create(&v)
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
		cl := t.hashRing.Get(fmt.Sprint(key))
		return nil, cl.Insert(&v)
	case oql.SelectOp:
		key := v.ID
		cl := t.hashRing.Get(fmt.Sprint(key))
		return cl.Select(&v)
	case oql.DeleteOp:
		return nil, fmt.Errorf("delete op not implpemented")
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}
