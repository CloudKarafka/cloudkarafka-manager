package dm

import (
	"github.com/84codes/cloudkarafka-mgmt/store"
)

type BrokerMetric struct {
	BytesInPerSec             int            `json:"bytes_in_per_sec"`
	BytesOutPerSec            int            `json:"bytes_out_per_sec"`
	MessagesInPerSec          int            `json:"messages_in_per_sec"`
	KafkaVersion              string         `json:"kafka_version"`
	BrokerId                  string         `json:"broker_id"`
	LeaderCount               int            `json:"leader_count"`
	PartitionCount            int            `json:"partition_count"`
	UnderReplicatedPartitions int            `json:"under_replicated_partitions"`
	ActiveController          bool           `json:"active_controller"`
	Connections               []SocketServer `json:"connections"`
}

type SocketServer struct {
	Interface                 string `json:"interface"`
	Processor                 string `json:"processor"`
	ConnectionCount           int    `json:"connection_count"`
	FailedAuthenticationTotal int    `json:"failed_authentication_total"`
}

type BrokerJvmMetric struct {
	KafkaVersion  string       `json:"kafka_version"`
	BrokerId      string       `json:"broker_id"`
	HeapMemory    store.Memory `json:"heap_memory"`
	NonHeapMemory store.Memory `json:"non_heap_memory"`
	YoungGen      store.Gc     `json:"young_gen"`
	OldGen        store.Gc     `json:"old_gen"`
}

type BrokerHealthMetric struct {
	BrokerId                     string                         `json:"broker_id"`
	UnderReplicatedPartitions    int                            `json:"under_replicated_partitions"`
	ControllerCount              int                            `json:"controller_count"`
	IsrDelta                     int                            `json:"isr_delta"`
	UncleanLeaderElectionsPerSec int                            `json:"unclean_leader_elections"`
	OfflinePartitionsCount       int                            `json:"offline_partitions"`
	Requests                     map[string]store.BrokerRequest `json:"requests"`
}

func SocketServers(brokerId string) []SocketServer {
	broker := store.Broker(brokerId)
	var (
		ss = make([]SocketServer, len(broker.Connections))
		i  = 0
	)
	for intf, count := range broker.Connections {
		ss[i] = SocketServer{
			Interface:       intf,
			ConnectionCount: count.Latest(),
		}
		i++
	}
	return ss
}

func BrokerMetrics(id string) BrokerMetric {
	broker := store.Broker(id)
	return BrokerMetric{
		KafkaVersion:     broker.Version,
		BrokerId:         id,
		Connections:      SocketServers(id),
		BytesInPerSec:    broker.BytesInPerSec.Latest(),
		BytesOutPerSec:   broker.BytesOutPerSec.Latest(),
		MessagesInPerSec: broker.MessagesInPerSec.Latest(),
		LeaderCount:      broker.LeaderCount,
		PartitionCount:   broker.PartitionCount,
		ActiveController: broker.ControllerCount == 1,
	}
}
func BrokerHealthMetrics(id string) BrokerHealthMetric {
	broker := store.Broker(id)
	requests := map[string]store.BrokerRequest{
		"producer":       broker.ProduceRequests,
		"fetch_consumer": broker.FetchConsumerRequests,
		"fetch_follower": broker.FetchFollowerRequests,
	}
	return BrokerHealthMetric{
		BrokerId:                     id,
		UnderReplicatedPartitions:    broker.UnderReplicatedPartitions,
		ControllerCount:              broker.ControllerCount,
		IsrDelta:                     broker.IsrDelta(),
		UncleanLeaderElectionsPerSec: broker.UncleanLeaderElectionsPerSec,
		OfflinePartitionsCount:       broker.OfflinePartitionsCount,
		Requests:                     requests,
	}
}

func BrokerJvmMetrics(id string) BrokerJvmMetric {
	broker := store.Broker(id)
	return BrokerJvmMetric{
		BrokerId:      id,
		HeapMemory:    broker.Jvm.Heap,
		NonHeapMemory: broker.Jvm.NonHeap,
		YoungGen:      broker.Jvm.YoungGen,
		OldGen:        broker.Jvm.OldGen,
	}
}

func AllBrokerBytesInPerSec() Series {
	brokers := store.Brokers()
	xy := make(map[int64]int)
	for _, id := range brokers {
		b := store.Broker(id)
		for ts, val := range b.BytesInPerSec {
			xy[ts] += val
		}
	}
	s := make(Series, len(xy))
	i := 0
	for x, y := range xy {
		s[i] = DataPoint{Y: y, X: x}
		i += 1
	}
	s.Sort()
	return s
}

func AllBrokerBytesOutPerSec() Series {
	brokers := store.Brokers()
	xy := make(map[int64]int)
	for _, id := range brokers {
		b := store.Broker(id)
		for ts, val := range b.BytesInPerSec {
			xy[ts] += val
		}
	}
	s := make(Series, len(xy))
	i := 0
	for x, y := range xy {
		s[i] = DataPoint{Y: y, X: x}
		i += 1
	}
	s.Sort()
	return s
}
