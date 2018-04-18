package dm

import (
	"cloudkarafka-mgmt/store"
	"fmt"
	"strings"
	//"cloudkarafka-mgmt/zookeeper"
)

type BrokerMetric struct {
	BytesInPerSec    int            `json:"bytes_in_per_sec"`
	BytesOutPerSec   int            `json:"bytes_out_per_sec"`
	MessagesInPerSec int            `json:"messages_in_per_sec"`
	KafkaVersion     string         `json:"kafka_version"`
	BrokerId         string         `json:"broker_id"`
	LeaderCount      int            `json:"leader_count"`
	PartitionCount   int            `json:"partition_count"`
	Connections      []SocketServer `json:"connections"`
}

type SocketServer struct {
	Interface                 string `json:"interface"`
	Processor                 string `json:"processor"`
	ConnectionCount           int    `json:"connection_count"`
	FailedAuthenticationTotal int    `json:"failed_authentication_total"`
}

func SocketServers(brokerId string) []SocketServer {
	grouped := store.Intersection(brokerId, "socket-server").GroupBy(func(d store.Data) string {
		return fmt.Sprintf("%s.%s.%s", d.Id["broker"], d.Id["listener"], d.Id["network_processor"])
	})
	socketServers := make([]SocketServer, len(grouped))
	i := 0
	for key, connections := range grouped {
		keys := strings.Split(key, ".")
		iface := keys[1]
		processor := keys[2]
		attributes := connections.GroupBy(func(d store.Data) string { return d.Id["attr"] })
		item := SocketServer{Interface: iface, Processor: processor}
		for attr, data := range attributes {
			ss := data.Stored[data.Len()-1]
			switch attr {
			case "connection-count":
				item.ConnectionCount = ss.Value
			case "failed-authentication-total":
				item.FailedAuthenticationTotal = ss.Value
			}
		}
		socketServers[i] = item
		i += 1
	}
	return socketServers
}

func BrokerMetrics(id string) BrokerMetric {
	bm := BrokerMetric{
		KafkaVersion: store.KafkaVersion[id],
		BrokerId:     id,
		Connections:  SocketServers(id),
	}
	brokerMetrics := store.SelectWithIndex(id).GroupByMetric()
	for metric, values := range brokerMetrics {
		values.Sort()
		value := values.Stored[values.Len()-1]
		switch metric {
		case "BytesInPerSec":
			bm.BytesInPerSec = value.Value
		case "BytesOutPerSec":
			bm.BytesOutPerSec = value.Value
		case "MessagesInPerSec":
			bm.MessagesInPerSec = value.Value
		case "LeaderCount":
			bm.LeaderCount = value.Value
		case "PartitionCount":
			bm.PartitionCount = value.Value
		}
	}
	return bm
}
