package hash

import (
	"crypto/sha1"
	"fmt"
)

type InnerRingNode struct {
	Next *InnerRingNode
	Addr string
	Hash uint32
}

type InnerRing struct {
	Head *InnerRingNode
}

type Shard struct {
	Id   string
	Ring InnerRing
}

type OuterRingNode struct {
	Next *OuterRingNode
	Val  *Shard
	Hash uint32
}

type OuterRing struct {
	VirtualShards   int
	Head            *OuterRingNode
	ReplicaPerShard int
	Namespace       string

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

func (t *OuterRing) add(shard string, id int) *Shard {
	iHead := &InnerRingNode{Addr: fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", shard, 0, shard, t.Namespace), Hash: hashKey("pod-0")}
	innerRing := InnerRing{Head: iHead}
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

func (t *OuterRing) GetNode(key string) string {
	if t.Head == nil {
		return ""
	}

	keyHash := hashKey(key)

	// 1. Find the shard (OuterRingNode) responsible for this key
	curr := t.Head
	var selected *OuterRingNode

	// Check if ring is circular
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

	// 2. Within selected shard, find the appropriate replica (InnerRingNode)
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

func (t *OuterRing) GetShard(key string) *Shard {
	if t.Head == nil {
		return nil
	}

	keyHash := hashKey(key)

	// 1. Find the shard (OuterRingNode) responsible for this key
	curr := t.Head
	var selected *OuterRingNode

	// Check if ring is circular
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

func (t *OuterRing) Remove(shard string) {
	for i := 0; i < t.VirtualShards; i++ {
		t.remove(shard, i)
	}
}

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
