package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/nagarajRPoojari/orangegate/internal/hash"
	log "github.com/nagarajRPoojari/orangegate/internal/utils/logger"

	"github.com/nagarajRPoojari/orange/pkg/query"
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
	t := &Proxy{Addr: addr, wt: *watch.NewWatcher()}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()

		if len(targets) == 0 {
			http.Error(w, "No targets available", http.StatusServiceUnavailable)
			return
		}

		target := targets[0]
		fullURL := t.mustParse(target)

		log.Infof(fullURL.String())

		var req Req

		err := json.NewDecoder(r.Body).Decode(&req)

		res, err := t.processQuery(req.Query)

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

	return t
}

func (t *Proxy) Serve() {
	http.ListenAndServe(t.Addr, nil)
}

func (t *Proxy) WatchShards() {
	go t.wt.Run(t.hashRing)
}

func (t *Proxy) mustParse(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		log.Warnf("Invalid URL:", s)
	}
	return u
}

func (t *Proxy) processQuery(q string) (any, error) {
	parser := query.NewParser(q)
	op, err := parser.Build()
	if err != nil {
		return nil, err
	}

	switch v := op.(type) {
	case query.CreateOp:
		for _, cl := range t.hashRing.GetAll() {
			cl.Create(&v)
		}
		return nil, nil
	case query.InsertOp:
		key := v.Value["_ID"].(int64)
		cl := t.hashRing.Get(fmt.Sprint(key))
		return nil, cl.Insert(&v)
	case query.SelectOp:
		key := v.ID
		cl := t.hashRing.Get(fmt.Sprint(key))
		return cl.Select(&v)
	case query.DeleteOp:
		return nil, fmt.Errorf("delete op not implpemented")
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}
