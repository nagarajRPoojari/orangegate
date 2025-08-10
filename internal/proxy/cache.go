package proxy

import "github.com/nagarajRPoojari/orange/net/client"

type Cache struct {
	conns map[string]*client.Client
}

func NewCache() *Cache {
	return &Cache{
		conns: map[string]*client.Client{},
	}
}

func (t *Cache) Get(addr string) *client.Client {
	if _, ok := t.conns[addr]; !ok {
		t.set(addr, client.NewClient(addr, 52001))
	}
	return t.conns[addr]
}

func (t *Cache) set(addr string, cl *client.Client) {
	t.conns[addr] = cl
}
