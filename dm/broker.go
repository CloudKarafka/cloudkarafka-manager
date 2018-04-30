package dm

import (
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"
	"fmt"
	"strings"
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
		return fmt.Sprintf("%s.%s.%s", d.Tags["broker"], d.Tags["listener"], d.Tags["network_processor"])
	})
	socketServers := make([]SocketServer, len(grouped))
	i := 0
	for key, connections := range grouped {
		keys := strings.Split(key, ".")
		iface := keys[1]
		processor := keys[2]
		attributes := connections.GroupBy(func(d store.Data) string { return d.Tags["attr"] })
		item := SocketServer{Interface: iface, Processor: processor}
		for attr, data := range attributes {
			ss := data.Last()
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
		value := values.Last()
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

func AllBrokerThroughputTimeseries(metric string) Series {
	brokers, _ := zookeeper.Brokers()
	xy := make(map[int64]int)
	for _, b := range brokers {
		for _, dp := range ThroughputTimeseries("BytesInPerSec", b) {
			xy[dp.X] += dp.Y
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
