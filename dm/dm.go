package dm

import (
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"

	"sort"
)

type BrokerMetric struct {
	BytesInPerSec    int    `json:"bytes_in_per_sec"`
	BytesOutPerSec   int    `json:"bytes_out_per_sec"`
	MessagesInPerSec int    `json:"messages_in_per_sec"`
	KafkaVersion     string `json:"kafka_version"`
	BrokerId         string `json:"broker_id"`
}

type TopicMetric struct {
	BytesInPerSec    int `json:"bytes_in_per_sec"`
	BytesOutPerSec   int `json:"bytes_out_per_sec"`
	MessagesInPerSec int `json:"messages_in_per_sec"`
	MessageCount     int `json:"message_count"`

	Partitions PartitionMetrics `json:"partitions"`
}

type PartitionMetric struct {
	Number         string `json:"number"`
	Leader         int    `json:"leader"`
	ISR            []int  `json:"isr"`
	LogStartOffset int    `json:"log_start_offset"`
	LogEndOffset   int    `json:"log_end_offset"`
}

type PartitionMetrics []PartitionMetric

func (pm PartitionMetrics) Len() int           { return len(pm) }
func (pm PartitionMetrics) Swap(i, j int)      { pm[i], pm[j] = pm[j], pm[i] }
func (pm PartitionMetrics) Less(i, j int) bool { return pm[i].Number < pm[j].Number }

func BrokerMetrics(id string) BrokerMetric {
	bm := BrokerMetric{KafkaVersion: jmx.KafkaVersion, BrokerId: id}
	brokerMetrics := store.SelectWithIndex(id).GroupByMetric()
	for metric, values := range brokerMetrics {
		values.Sort()
		value := values[len(values)-1]
		switch metric {
		case "byte_in_per_sec":
			bm.BytesInPerSec = value.Value
		case "byte_out_per_sec":
			bm.BytesOutPerSec = value.Value
		case "messages_in_per_sec":
			bm.MessagesInPerSec = value.Value
		}
	}
	return bm
}

func TopicMetrics(name string) TopicMetric {
	tm := TopicMetric{}
	partitions := store.SelectWithIndex(name).GroupByPartition()
	total := 0
	for partition, data := range partitions {
		value := data[len(data)-1]
		if partition == "" {
			tm.BytesInPerSec = value.Value
			tm.BytesOutPerSec = value.Value
			tm.MessagesInPerSec = value.Value
		} else {
			p, _ := zookeeper.Partition(name, partition)
			pm := PartitionMetric{
				Number: p.Number,
				Leader: p.Leader,
				ISR:    p.Isr,
			}
			for _, m := range data {
				switch m.Id["metric"] {
				case "log_start_offset":
					pm.LogStartOffset = m.Value
				case "log_end_offset":
					pm.LogEndOffset = m.Value
				}
			}
			tm.Partitions = append(tm.Partitions, pm)
			total += pm.LogEndOffset - pm.LogStartOffset
		}
	}
	sort.Sort(tm.Partitions)
	tm.MessageCount = total
	return tm
}
