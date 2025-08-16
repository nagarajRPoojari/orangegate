package main

import (
	"github.com/nagarajRPoojari/orangectl/internal/proxy"
	log "github.com/nagarajRPoojari/orangectl/internal/utils/logger"
)

// orangectl should only be used within a k8s environment in prod mode
// since it is currently tightly coupled with k8s environment
func main() {
	log.Disable()

	p := proxy.NewProxy(":8000")
	p.WatchShards()
	p.Serve()
}
