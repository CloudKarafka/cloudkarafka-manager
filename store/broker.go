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
type Gc struct {
	CollectionTime  int `json:"collection_time"`
	CollectionCount int `json:"collection_count"`
}
type jvm struct {
	Heap     Memory
	NonHeap  Memory
	YoungGen Gc
	OldGen   Gc
}

// StdDev:0 75thPercentile:0 98thPercentile:0 95thPercentile:0 99thPercentile:0 Max:0 Mean:0 Min:0 BrokerId:0 999thPercentile:0 Count:0 50thPercentile:0
type BrokerRequestValue struct {
	Max           float64
	Mean          float64
	Min           float64
	Count         float64
	Percentile50  float64
	Percentile75  float64
	Percentile95  float64
	Percentile98  float64
	Percentile99  float64
	Percentile999 float64
}

func NewBrokerRequestValue(source map[string]interface{}) BrokerRequestValue {
	return BrokerRequestValue{
		source["Max"].(float64),
		source["Mean"].(float64),
		source["Min"].(float64),
		source["Count"].(float64),
		source["50thPercentile"].(float64),
		source["75thPercentile"].(float64),
		source["95thPercentile"].(float64),
		source["98thPercentile"].(float64),
		source["99thPercentile"].(float64),
		source["999thPercentile"].(float64),
	}
}

type BrokerRequest struct {
	Requests      int                `json:"requests"`
	RequestQueue  BrokerRequestValue `json:"request_queue_time"`
	LocalTime     BrokerRequestValue `json:"local_time"`
	RemoteTime    BrokerRequestValue `json:"remote_time"`
	ResponseQueue BrokerRequestValue `json:"response_queue_time"`
	RespondSend   BrokerRequestValue `json:"respond_send_time"`
}

type broker struct {
	Version                      string
	Connections                  map[string]Timeseries
	LeaderCount                  int
	PartitionCount               int
	UnderReplicatedPartitions    int
	ControllerCount              int
	OfflinePartitionsCount       int
	IsrShrinksPerSec             int
	IsrExpandsPerSec             int
	UncleanLeaderElectionsPerSec int
	BytesInPerSec                Timeseries
	BytesOutPerSec               Timeseries
	MessagesInPerSec             Timeseries
	Jvm                          jvm
	ProduceRequests              BrokerRequest
	FetchConsumerRequests        BrokerRequest
	FetchFollowerRequests        BrokerRequest
}

type brokerStore struct {
	sync.RWMutex
	store map[string]broker
}

func (b *broker) IsrDelta() int {
	return b.IsrShrinksPerSec - b.IsrExpandsPerSec
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
func (me *brokerStore) ControllerCount(brokerId string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.ControllerCount = value
	me.store[brokerId] = b
}

func (me *brokerStore) OfflinePartitionsCount(brokerId string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.OfflinePartitionsCount = value
	me.store[brokerId] = b
}

func (me *brokerStore) IsrExpandsPerSec(brokerId string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.IsrExpandsPerSec = value
	me.store[brokerId] = b
}

func (me *brokerStore) IsrShrinksPerSec(brokerId string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.IsrShrinksPerSec = value
	me.store[brokerId] = b
}

func (me *brokerStore) UncleanLeaderElectionsPerSec(brokerId string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	b.UncleanLeaderElectionsPerSec = value
	me.store[brokerId] = b
}

func updateRequestTimes(req *BrokerRequest, name string, value BrokerRequestValue) {
	switch name {
	case "RequestQueueTimeMs":
		req.RequestQueue = value
	case "LocalTimeMs":
		req.LocalTime = value
	case "RemoteTimeMs":
		req.RemoteTime = value
	case "ResponseQueueTimeMs":
		req.ResponseQueue = value
	case "ResponseSendTimeMs":
		req.RespondSend = value
	}
}
func (me *brokerStore) RequestCount(brokerId string, value int, request string) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	switch request {
	case "Produce":
		b.ProduceRequests.Requests = value
	case "FetchConsumer":
		b.FetchConsumerRequests.Requests = value
	case "FetchFollower":
		b.FetchFollowerRequests.Requests = value
	}
	me.store[brokerId] = b
}
func (me *brokerStore) RequestTime(brokerId string, value BrokerRequestValue, name, request string) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	switch request {
	case "Produce":
		updateRequestTimes(&b.ProduceRequests, name, value)
	case "FetchConsumer":
		updateRequestTimes(&b.FetchConsumerRequests, name, value)
	case "FetchFollower":
		updateRequestTimes(&b.FetchFollowerRequests, name, value)
	}
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

func (me *brokerStore) JvmGc(brokerId, space, key string, value int) {
	me.Lock()
	defer me.Unlock()
	b := me.store[brokerId]
	var gc *Gc
	switch space {
	case "G1 Young Generation":
		gc = &b.Jvm.YoungGen
	case "G1 Old Generation":
		gc = &b.Jvm.OldGen
	default:
		return
	}
	switch key {
	case "CollectionCount":
		gc.CollectionCount = value
	case "CollectionTime":
		gc.CollectionTime = value
	}
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
