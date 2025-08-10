package main

import "github.com/nagarajRPoojari/orangectl/internal/proxy"

func main() {
	p := proxy.NewProxy(":8000")
	p.WatchShards()
	p.Serve()
}
