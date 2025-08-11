package proxy

import "github.com/nagarajRPoojari/orange/net/client"

// Cache manages a mapping from addresses to client connections,
// allowing reuse of clients instead of creating new ones repeatedly.
type Cache struct {
	conns map[string]*client.Client
}

// NewCache initializes and returns a new Cache instance.
func NewCache() *Cache {
	return &Cache{
		conns: map[string]*client.Client{},
	}
}

// Get returns a client.Client associated with the given address.
// If the client does not exist yet, it creates a new one, stores it in the cache,
// and then returns it.
func (t *Cache) Get(addr string) *client.Client {
	if _, ok := t.conns[addr]; !ok {
		// NewClient tries to establish a connection with server
		// and keeps retrying with exponential backfoff
		t.set(addr, client.NewClient(addr, 52001, 10))
	}
	return t.conns[addr]
}

// set adds or updates the client.Client for the specified address in the cache.
func (t *Cache) set(addr string, cl *client.Client) {
	t.conns[addr] = cl
}
