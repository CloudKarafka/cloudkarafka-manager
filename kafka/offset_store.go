package kafka

import (
	"cloudkarafka-mgmt/zookeeper"

	"sync"
)

var (
	l           = new(sync.Mutex)
	OffsetStore = make(map[string]ConsumerGroup)
)

type ConsumerGroup map[string]ConsumedPartition

type ConsumedPartition map[int]Offset

type Offset struct {
	Timestamp int64
	Offset    int
}

func Consumers(p zookeeper.Permissions) []string {
	l.Lock()
	defer l.Unlock()
	consumers := make([]string, len(OffsetStore))
	i := 0
	for c, _ := range OffsetStore {
		if p.GroupRead(c) {
			consumers[i] = c
			i += 1
		}
	}
	return consumers

}

func Consumer(c string, p zookeeper.Permissions) ConsumerGroup {
	l.Lock()
	defer l.Unlock()
	var os ConsumerGroup
	if p.GroupRead(c) {
		os = OffsetStore[c]
	}
	return os
}

func store(msg message) {
	l.Lock()
	defer l.Unlock()
	if _, ok := OffsetStore[msg.Group]; !ok {
		OffsetStore[msg.Group] = make(map[string]ConsumedPartition)
	}
	if _, ok := OffsetStore[msg.Group][msg.Topic]; !ok {
		OffsetStore[msg.Group][msg.Topic] = make(ConsumedPartition)
	}
	OffsetStore[msg.Group][msg.Topic][msg.Partition] = Offset{
		Timestamp: msg.Timestamp,
		Offset:    msg.Offset,
	}
}
