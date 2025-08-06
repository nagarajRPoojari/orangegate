package hash

import (
	"crypto/sha1"
	"sort"
	"strconv"

	"github.com/nagarajRPoojari/orange/net/client"
)

// HashRing is the structure for consistent hashing
type HashRing struct {
	// number of virtual nodes per shard-replica
	replicas int
	// list of hash values of all nodes (including virtual) for easy binary search
	keys []int
	// map of real node hash with its grpc client for broadcast
	allNodes map[int]*client.Client

	// hashring with all nodes
	hashMap map[int]*client.Client
}

// NewHashRing initializes a HashRing
func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		hashMap:  make(map[int]*client.Client),
	}
}

// Add adds a node to the hash ring
func (h *HashRing) Add(addr string, port int64) {
	cl := client.NewClient(addr, port)
	for i := 0; i < h.replicas; i++ {
		hash := int(hashKey(addr + strconv.Itoa(i)))
		h.keys = append(h.keys, hash)
		h.hashMap[hash] = cl
	}
	hash := int(hashKey(addr))
	h.allNodes[hash] = cl
	sort.Ints(h.keys)
}

// Remove deletes a node and its replicas from the ring
func (h *HashRing) Remove(node string) {
	for i := 0; i < h.replicas; i++ {
		replicaHash := int(hashKey(node + strconv.Itoa(i)))
		shardHash := int(hashKey(node))
		delete(h.hashMap, replicaHash)
		delete(h.allNodes, shardHash)
		// Remove from keys slice
		for j, k := range h.keys {
			if k == replicaHash {
				h.keys = append(h.keys[:j], h.keys[j+1:]...)
				break
			}
		}
	}
}

// Get returns the closest node in the hash ring for the given key
func (h *HashRing) Get(key string) *client.Client {
	if len(h.keys) == 0 {
		return nil
	}
	hash := int(hashKey(key))
	// Binary search for appropriate replica
	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})
	if idx == len(h.keys) {
		idx = 0
	}
	return h.hashMap[h.keys[idx]]
}

func (h *HashRing) GetAll() map[int]*client.Client {
	return h.allNodes
}

// hashKey generates a consistent SHA-1 hash
func hashKey(key string) uint32 {
	h := sha1.New()
	h.Write([]byte(key))
	bs := h.Sum(nil)
	// Use first 4 bytes to form a uint32 hash
	return uint32(bs[0])<<24 | uint32(bs[1])<<16 | uint32(bs[2])<<8 | uint32(bs[3])
}
