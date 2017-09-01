package kafka

import (
	"cloudkarafka-mgmt/zookeeper"
	"log"

	"sync"
	"time"
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

func purgeOldConsumers() {
	l.Lock()
	defer l.Unlock()
	activeConsumers := make(map[string]ConsumerGroup)
	for c, cg := range OffsetStore {
		for t, cp := range cg {
			for p, off := range cp {
				if 1*time.Minute <= time.Since(time.Unix(off.Timestamp/1000, 0)) {
					continue
				}
				if _, ok := activeConsumers[c]; !ok {
					activeConsumers[c] = make(map[string]ConsumedPartition)
				}
				if _, ok := activeConsumers[c][t]; !ok {
					activeConsumers[c][t] = make(ConsumedPartition)
				}
				activeConsumers[c][t][p] = off
			}
		}
	}
	OffsetStore = activeConsumers
}

func init() {
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			start := time.Now()
			log.Println("[INFO] Purge offset store started")
			purgeOldConsumers()
			log.Printf("[INFO] Purge offset store ended, it took %s", time.Since(start))
		}
	}()
}
