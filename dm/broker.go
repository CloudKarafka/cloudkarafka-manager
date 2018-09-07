package dm

import (
	"cloudkarafka-mgmt/store"
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
		KafkaVersion:              broker.Version,
		BrokerId:                  id,
		Connections:               SocketServers(id),
		BytesInPerSec:             broker.BytesInPerSec.Latest(),
		BytesOutPerSec:            broker.BytesOutPerSec.Latest(),
		MessagesInPerSec:          broker.MessagesInPerSec.Latest(),
		LeaderCount:               broker.LeaderCount,
		PartitionCount:            broker.PartitionCount,
		UnderReplicatedPartitions: broker.UnderReplicatedPartitions,
		ActiveController:          broker.ActiveController,
	}
}

func BrokerJvmMetrics(id string) BrokerJvmMetric {
	broker := store.Broker(id)
	return BrokerJvmMetric{
		KafkaVersion:  broker.Version,
		BrokerId:      id,
		HeapMemory:    broker.Jvm.Heap,
		NonHeapMemory: broker.Jvm.NonHeap,
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
