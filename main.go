package main

import "github.com/nagarajRPoojari/orangegate/internal/proxy"

func main() {
	p := proxy.NewProxy(":8000")
	p.WatchShards()
	p.Serve()
}
