package store

import (
	"sync"
)

type partition struct {
	EndOffset   int
	StartOffset int
	Size        int
}

type topic struct {
	BytesInPerSec    Timeseries
	BytesOutPerSec   Timeseries
	MessagesInPerSec Timeseries
	Partitions       map[string]partition
}

type topicStore struct {
	sync.RWMutex
	store map[string]topic
}

func (me *topicStore) partition(topicName, partitionName string) (topic, partition) {
	if _, ok := me.store[topicName]; !ok {
		me.store[topicName] = topic{Partitions: make(map[string]partition)}
	}
	t := me.store[topicName]
	if _, ok := t.Partitions[partitionName]; !ok {
		t.Partitions[partitionName] = partition{}
	}
	return t, t.Partitions[partitionName]
}

func (me *topicStore) LogStartOffset(topic, partition string, value int, _ts int64) {
	me.Lock()
	defer me.Unlock()
	t, p := me.partition(topic, partition)
	p.StartOffset = value
	t.Partitions[partition] = p
	me.store[topic] = t
}

func (me *topicStore) LogEndOffset(topic, partition string, value int, _ts int64) {
	me.Lock()
	defer me.Unlock()
	t, p := me.partition(topic, partition)
	p.EndOffset = value
	t.Partitions[partition] = p
	me.store[topic] = t
}

func (me *topicStore) Size(topic, partition string, value int) {
	me.Lock()
	defer me.Unlock()
	t, p := me.partition(topic, partition)
	p.Size = value
	t.Partitions[partition] = p
	me.store[topic] = t
}

func (me *topicStore) BytesInPerSec(topic string, value int, ts int64) {
	me.Lock()
	defer me.Unlock()
	t := me.store[topic]
	t.BytesInPerSec = t.BytesInPerSec.Add(value, ts)
	me.store[topic] = t
}

func (me *topicStore) BytesOutPerSec(topic string, value int, ts int64) {
	me.Lock()
	defer me.Unlock()
	t := me.store[topic]
	t.BytesOutPerSec = t.BytesOutPerSec.Add(value, ts)
	me.store[topic] = t
}

func (me *topicStore) MessagesInPerSec(topic string, value int, ts int64) {
	me.Lock()
	defer me.Unlock()
	t := me.store[topic]
	t.MessagesInPerSec = t.MessagesInPerSec.Add(value, ts)
	me.store[topic] = t
}

func (me *topicStore) Topics() []string {
	me.RLock()
	defer me.RUnlock()
	var (
		topics = make([]string, len(me.store))
		i      = 0
	)
	for t, _ := range me.store {
		topics[i] = t
		i++
	}
	return topics
}

func (me *topicStore) Topic(name string) topic {
	me.RLock()
	defer me.RUnlock()
	return me.store[name]
}
