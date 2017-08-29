package kafka

import (
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

func Consumers() []string {
	l.Lock()
	defer l.Unlock()
	consumers := make([]string, len(OffsetStore))
	i := 0
	for c, _ := range OffsetStore {
		consumers[i] = c
		i += 1
	}
	return consumers

}

func Consumer(c string) ConsumerGroup {
	l.Lock()
	defer l.Unlock()
	return OffsetStore[c]
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
