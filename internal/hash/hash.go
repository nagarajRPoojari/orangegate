package hash

import (
	"crypto/sha1"
	"fmt"
)

type InnerRingNode struct {
	Next *InnerRingNode
	// address of individual pod/replica within a shard
	Addr string
	Hash uint32
}

// InnerRing represents consistent hash ring with pods(replicas) of a shard
// Note:
//
//   - only writes uses inner consistent ring to avoid concurrent conflicting
//     updates of same key on different replicas.
//   - read calls should use quorum reads based on consistency level specifed
type InnerRing struct {
	Head *InnerRingNode
}

// Shard represents group of replicas/pods
type Shard struct {
	Id   string
	Ring InnerRing
}

// OuterRingNode represents consistent hash ring with all shards
type OuterRingNode struct {
	Next *OuterRingNode
	Val  *Shard
	Hash uint32
}

// OuterRing represents a ring of virtual shards for consistent hashing
type OuterRing struct {
	// VirtualShards is the total number of virtual shards in the ring.
	// used to distribute data more evenly across nodes.
	VirtualShards int

	// Head is a pointer to the first node in the circular linked list
	// representing the outer ring.
	Head *OuterRingNode

	// ReplicaPerShard defines how many replicas each shard should have.
	ReplicaPerShard int

	// k8s namespace within which pods are deployed, used to dynamically build
	// dns address of pods
	Namespace string

	// allShards maps shard identifiers to their corresponding Shard structs.
	allShards map[string]*Shard
}

func NewHashRing(virtualShards, replicaPerShard int, namespace string) *OuterRing {
	return &OuterRing{VirtualShards: virtualShards, ReplicaPerShard: replicaPerShard, Namespace: namespace, allShards: make(map[string]*Shard)}
}

func (t *OuterRing) Add(shard string) {
	var sh *Shard
	for i := 0; i < t.VirtualShards; i++ {
		sh = t.add(shard, i)
	}
	t.allShards[shard] = sh
}

func (t *OuterRing) GetAllShards() map[string]*Shard {
	return t.allShards
}

// add inserts a virtual shard node into the OuterRing along with its corresponding replicas
// in the shard's inner ring. The `shard` parameter specifies the shard's identifier, and `id`
// is the virtual shard ID used for hashing and placement in the ring. This method creates
// the inner ring of replicas (pods) for the shard based on ReplicaPerShard and inserts the
// new OuterRingNode in sorted order by hash in the outer ring.
func (t *OuterRing) add(shard string, id int) *Shard {
	// e.g addres: shard-1-0.shard-1.svc.cluster.local
	iHead := &InnerRingNode{Addr: fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", shard, 0, shard, t.Namespace), Hash: hashKey("pod-0")}
	innerRing := InnerRing{Head: iHead}

	// constructing inner hash ring with specified number of replica per shard
	for i := 1; i < t.ReplicaPerShard; i++ {
		iHead.Next = &InnerRingNode{
			Addr: fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", shard, i, shard, t.Namespace),
			Hash: hashKey(fmt.Sprintf("pod-%d", i)),
		}
		iHead = iHead.Next
	}

	node := &OuterRingNode{
		Val:  &Shard{Id: shard, Ring: innerRing},
		Hash: hashKey(fmt.Sprintf("%s-%d", shard, id)),
	}

	if t.Head == nil {
		t.Head = node
		return node.Val
	}
	if node.Hash < t.Head.Hash {
		node.Next = t.Head
		t.Head = node
		return node.Val
	}

	first, second := t.Head, t.Head.Next
	for {
		if second == nil || second.Hash > node.Hash {
			first.Next = node
			node.Next = second
			return node.Val
		}
		first = second
		second = second.Next
	}
}

// GetNode returns the address (e.g., pod or replica) responsible for the given key.
// It first identifies the appropriate shard (OuterRingNode) using consistent hashing,
// then selects the correct replica (InnerRingNode) within that shard's inner ring.
// If the ring is uninitialized or incomplete, it returns an empty str
func (t *OuterRing) GetNode(key string) string {
	if t.Head == nil {
		return ""
	}

	keyHash := hashKey(key)

	// find the shard (OuterRingNode) responsible for this key
	curr := t.Head
	var selected *OuterRingNode

	// check if ring is circular
	if curr.Next == nil {
		selected = curr
	} else {
		for {
			if keyHash <= curr.Hash {
				selected = curr
				break
			}
			if curr.Next == nil {
				selected = t.Head
				break
			}
			curr = curr.Next
		}
	}

	// within selected shard, find the appropriate replica (InnerRingNode)
	if selected == nil || selected.Val == nil || selected.Val.Ring.Head == nil {
		return ""
	}

	innerHead := selected.Val.Ring.Head
	inner := innerHead
	for {
		if keyHash <= inner.Hash {
			return inner.Addr
		}
		inner = inner.Next
		if inner == nil {
			return innerHead.Addr
		}
	}
}

// GetShard returns the shard responsible for the given key by performing
// consistent hashing on the key and traversing the ring to find the
// appropriate OuterRingNode. If the ring is empty, it returns nil.
func (t *OuterRing) GetShard(key string) *Shard {
	if t.Head == nil {
		return nil
	}

	keyHash := hashKey(key)

	// find the shard (OuterRingNode) responsible for this key
	curr := t.Head
	var selected *OuterRingNode

	// check if ring is circular
	if curr.Next == nil {
		selected = curr
	} else {
		for {
			if keyHash <= curr.Hash {
				selected = curr
				break
			}
			if curr.Next == nil {
				selected = t.Head
				break
			}
			curr = curr.Next
		}
	}
	return selected.Val
}

// Remove removes all virtual shard instances associated with the given shard name
// from the outer ring. It does this by iterating over all virtual shard IDs
// and calling the internal remove function.
func (t *OuterRing) Remove(shard string) {
	for i := 0; i < t.VirtualShards; i++ {
		t.remove(shard, i)
	}
}

// remove removes a specific virtual shard identified by its shard name and ID
// from the ring. It handles removal of the head node separately and updates
// the linked list accordingly to maintain ring structure.
func (t *OuterRing) remove(shard string, id int) {
	if t.Head == nil {
		return
	}
	keyHash := hashKey(fmt.Sprintf("%s-%d", shard, id))
	if t.Head.Hash == keyHash {
		t.Head = t.Head.Next
		return
	}

	prev := t.Head
	for curr := t.Head.Next; curr != nil; {
		if curr.Hash == keyHash {
			prev.Next = curr.Next
			return
		}
		prev = curr
		curr = curr.Next
	}
}

func hashKey(key string) uint32 {
	h := sha1.New()
	h.Write([]byte(key))
	bs := h.Sum(nil)
	// Use first 4 bytes to form a uint32 hash
	return uint32(bs[0])<<24 | uint32(bs[1])<<16 | uint32(bs[2])<<8 | uint32(bs[3])
}
