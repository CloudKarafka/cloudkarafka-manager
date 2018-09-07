package store

import (
	"fmt"
)

var (
	cs = &consumerStore{store: make(map[string]consumedTopics)}
	bs = &brokerStore{store: make(map[string]broker)}
	ts = &topicStore{store: make(map[string]topic)}
)

func Put(metric string, value int, timestamp int64, keys ...string) {
	switch metric {
	case "broker":
		switch keys[0] {
		case "KafkaVersion":
			bs.Version(keys[1], keys[2])
		case "ConnectionCount":
			bs.Connections(keys[1], keys[2], value)
		case "BytesInPerSec":
			bs.BytesInPerSec(keys[1], value, timestamp)
		case "BytesOutPerSec":
			bs.BytesOutPerSec(keys[1], value, timestamp)
		case "MessagesInPerSec":
			bs.MessagesInPerSec(keys[1], value, timestamp)
		case "LeaderCount":
			bs.LeaderCount(keys[1], value, timestamp)
		case "PartitionCount":
			bs.PartitionCount(keys[1], value, timestamp)
		case "ReplicationBytesInPerSec":
		case "FailedIsrUpdatesPerSec":
		case "FailedProduceRequestsPerSec":
		case "TotalFetchRequestsPerSec":
		case "IsrExpandsPerSec":
		case "RequestQueueSize":
		case "BytesRejectedPerSec":
		case "IsrShrinksPerSec":
		case "TotalTimeMs":
		case "ReplicationBytesOutPerSec":
		case "UnderReplicatedPartitions":
			bs.UnderReplicatedPartitions(keys[1], value, timestamp)
		case "TotalProduceRequestsPerSec":
		case "FailedFetchRequestsPerSec":
		case "ProduceMessageConversionsPerSec":
		case "OfflineReplicaCount":
		case "UnderMinIsrPartitionCount":
		case "FetchMessageConversionsPerSec":
		case "ActiveControllerCount":
			bs.ActiveController(keys[1], value == 1)
		case "GlobalPartitionCount":
		case "PreferredReplicaImbalanceCount":
		case "OfflinePartitionsCount":
		case "ControllerState":
		case "GlobalTopicCount":
		default:
			fmt.Printf("[INFO] Unknown key (broker) (%s)\n", keys[0])
		}
	case "topic":
		switch keys[0] {
		case "LogStartOffset":
			ts.LogStartOffset(keys[1], keys[2], value, timestamp)
		case "LogEndOffset":
			ts.LogEndOffset(keys[1], keys[2], value, timestamp)
		case "Size":
			ts.Size(keys[1], keys[2], value)
		case "BytesInPerSec":
			ts.BytesInPerSec(keys[1], value, timestamp)
		case "BytesOutPerSec":
			ts.BytesOutPerSec(keys[1], value, timestamp)
		case "MessagesInPerSec":
			ts.MessagesInPerSec(keys[1], value, timestamp)
		case "NumLogSegments":
		case "TotalFetchRequestsPerSec":
		case "time-since-last-run-ms":
		case "FailedProduceRequestsPerSec":
		case "max-dirty-percent":
		case "BytesRejectedPerSec":
		case "cleaner-recopy-percent":
		case "TotalProduceRequestsPerSec":
		case "FailedFetchRequestsPerSec":
		case "max-clean-time-secs":
		case "max-buffer-utilization-percent":
		case "LogDirectoryOffline":
		case "OfflineLogDirectoryCount":
		case "FetchMessageConversionsPerSec":
		case "ProduceMessageConversionsPerSec":
		default:
			fmt.Printf("[INFO] Unknown key (topic) (%s)\n", keys[0])
		}
	case "consumer":
		cs.Put(value, timestamp, keys[0], keys[1], keys[2])
	case "jvm":
		switch keys[1] {
		case "HeapMemoryUsage":
			bs.HeapMemory(keys[0], keys[2], value)
		case "NonHeapMemoryUsage":
			bs.NonHeapMemory(keys[0], keys[2], value)
		default:
			fmt.Printf("[INFO] Unknown key (jvm) (%s)\n", keys[0])
		}
	default:
		fmt.Printf("[INFO] Unknown metric (%s)\n", metric)
	}
}

func Brokers() []string {
	return bs.Brokers()
}

func Broker(id string) broker {
	return bs.Broker(id)
}

func Topics() []string {
	return ts.Topics()
}

func Topic(name string) topic {
	return ts.Topic(name)
}

func Consumers() []string {
	return cs.Consumers()
}

func Consumer(name string) consumedTopics {
	return cs.Consumer(name)
}
