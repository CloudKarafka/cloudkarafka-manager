package dm

import (
	"cloudkarafka-mgmt/store"
	//"cloudkarafka-mgmt/zookeeper"
)

type BrokerMetric struct {
	BytesInPerSec    int    `json:"bytes_in_per_sec"`
	BytesOutPerSec   int    `json:"bytes_out_per_sec"`
	MessagesInPerSec int    `json:"messages_in_per_sec"`
	KafkaVersion     string `json:"kafka_version"`
	BrokerId         string `json:"broker_id"`
	LeaderCount      int    `json:"leader_count"`
	PartitionCount   int    `json:"partition_count"`
}

type SocketServer struct {
	Interface                 string `json:"interface"`
	BrokerId                  string `json:"broker_id"`
	Processor                 string `json:"processor"`
	ConnectionCount           int    `json:"connection_count"`
	FailedAuthenticationTotal int    `json:"failed_authentication_total"`
}

/*func SocketServers() []SocketServer {
	grouped := store.SelectWithIndex("socket-server").GroupBy(func(d store.Data) {
		return fmt.Sprintf("%s.%s.%s", d.Id["broker"], d.Id["listener"], d.Id["network_processor"])
	})
	socketServers := make([]SocketServer, len(grouped))
	i := 0
	for _, store := range grouped {
		store.Sort()
		ss := store.Stored[store.Len()-1]
		socketServers[i] = SocketServer{
			Interface: ss.Id["listener"],
		}
		i += 1
	}
	return socketServers
}*/

func BrokerMetrics(id string) BrokerMetric {
	bm := BrokerMetric{
		KafkaVersion: store.KafkaVersion[id],
		BrokerId:     id,
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
