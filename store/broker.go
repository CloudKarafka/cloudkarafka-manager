package store

import (
	"sync"
)

type Memory struct {
	Init     int `json:"init"`
	Commited int `json:"commited"`
	Max      int `json:"max"`
	Used     int `json:"used"`
}
type jvm struct {
	Heap    Memory
	NonHeap Memory
}

type broker struct {
	Version                   string
	Connections               map[string]Timeseries
	LeaderCount               int
	PartitionCount            int
	UnderReplicatedPartitions int
	BytesInPerSec             Timeseries
	BytesOutPerSec            Timeseries
	MessagesInPerSec          Timeseries
	Jvm                       jvm
}

type brokerStore struct {
	sync.RWMutex
	store map[string]broker
}

func (me *brokerStore) Version(brokerId, version string) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.Version = version
	me.store[brokerId] = b
}

func (me *brokerStore) Connections(brokerId, intf string, count int) {
	me.Lock()
	defer me.Unlock()
	if _, ok := me.store[brokerId]; !ok {
		me.store[brokerId] = broker{Connections: make(map[string]Timeseries)}
	}
	b := me.store[brokerId]
	if b.Connections == nil {
		b.Connections = make(map[string]Timeseries)
	}
	if _, ok := b.Connections[intf]; !ok {
		b.Connections[intf] = make(Timeseries, 0)
	}
	ts := b.Connections[intf]
	ts.Add(count, 0)
	b.Connections[intf] = ts
	me.store[brokerId] = b
}

func (me *brokerStore) BytesInPerSec(brokerId string, value int, ts int64) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.BytesInPerSec = b.BytesInPerSec.Add(value, ts)
	me.store[brokerId] = b
}

func (me *brokerStore) BytesOutPerSec(brokerId string, value int, ts int64) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.BytesOutPerSec = b.BytesOutPerSec.Add(value, ts)
	me.store[brokerId] = b
}

func (me *brokerStore) MessagesInPerSec(brokerId string, value int, ts int64) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.MessagesInPerSec = b.MessagesInPerSec.Add(value, ts)
	me.store[brokerId] = b
}

func (me *brokerStore) LeaderCount(brokerId string, value int, _ts int64) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.LeaderCount = value
	me.store[brokerId] = b
}

func (me *brokerStore) PartitionCount(brokerId string, value int, _ts int64) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.PartitionCount = value
	me.store[brokerId] = b
}
func (me *brokerStore) UnderReplicatedPartitions(brokerId string, value int, _ts int64) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.UnderReplicatedPartitions = value
	me.store[brokerId] = b
}

func updateMemory(mem *Memory, memoryKey string, value int) {
	switch memoryKey {
	case "init":
		mem.Init = value
	case "committed":
		mem.Commited = value
	case "used":
		mem.Used = value
	case "max":
		mem.Max = value
	}
}
func (me *brokerStore) HeapMemory(brokerId, memoryKey string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	updateMemory(&b.Jvm.Heap, memoryKey, value)
	me.store[brokerId] = b
}
func (me *brokerStore) NonHeapMemory(brokerId, memoryKey string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	updateMemory(&b.Jvm.NonHeap, memoryKey, value)
	me.store[brokerId] = b
}

func (me *brokerStore) Brokers() []string {
	me.RLock()
	defer me.RUnlock()
	var (
		brokers = make([]string, len(me.store))
		i       = 0
	)
	for b, _ := range me.store {
		brokers[i] = b
		i += 1
	}
	return brokers
}

func (me *brokerStore) Broker(id string) broker {
	me.RLock()
	defer me.RUnlock()
	return me.store[id]
}
