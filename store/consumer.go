package store

import (
	"sync"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
)

type consumedPartitions map[string]Timeseries
type consumedTopics map[string]consumedPartitions

type consumerStore struct {
	sync.RWMutex
	store map[string]consumedTopics
}

func (me *consumerStore) Put(value int, ts int64, group, topic, partition string) error {
	me.Lock()
	defer me.Unlock()
	if _, ok := me.store[group]; !ok {
		me.store[group] = make(consumedTopics)
	}
	g := me.store[group]
	if _, ok := g[topic]; !ok {
		g[topic] = make(consumedPartitions)
	}
	ct := g[topic]
	t := Topic(topic)
	p := t.Partitions[partition]
	lag := p.EndOffset - value
	if lag < 0 {
		lag = 0
	}
	ct[partition] = ct[partition].Add(lag, ts)
	ct[partition].RemoveEntriesOlderThan(time.Now().Unix() - config.Retention)
	g[topic] = ct
	me.store[group] = g
	return nil
}

func (c consumerStore) Consumers() []string {
	c.RLock()
	defer c.RUnlock()
	var (
		names = make([]string, len(c.store))
		i     = 0
	)
	for name, _ := range c.store {
		names[i] = name
		i += 1
	}
	return names
}

func (c consumerStore) Consumer(name string) consumedTopics {
	c.RLock()
	defer c.RUnlock()
	return c.store[name]
}
