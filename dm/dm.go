package dm

import (
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"

	"sort"
	"strconv"
)

type TopicMetric struct {
	BytesInPerSec    int `json:"bytes_in_per_sec"`
	BytesOutPerSec   int `json:"bytes_out_per_sec"`
	MessagesInPerSec int `json:"messages_in_per_sec"`
	MessageCount     int `json:"message_count"`
	Size             int `json:"size"`

	PMs PartitionMetrics `json:"partition_metrics"`
}

type PartitionMetric struct {
	Number         string `json:"number"`
	Leader         int    `json:"leader"`
	ISR            []int  `json:"isr"`
	LogStartOffset int    `json:"log_start_offset"`
	LogEndOffset   int    `json:"log_end_offset"`
	Size           int    `json:"size"`
}

type PartitionMetrics []PartitionMetric

func (pm PartitionMetrics) Len() int      { return len(pm) }
func (pm PartitionMetrics) Swap(i, j int) { pm[i], pm[j] = pm[j], pm[i] }
func (pm PartitionMetrics) Less(i, j int) bool {
	in, _ := strconv.Atoi(pm[i].Number)
	jn, _ := strconv.Atoi(pm[j].Number)
	return in < jn
}

func TopicMetrics(name string) TopicMetric {
	tm := TopicMetric{}
	partitions := store.SelectWithIndex(name).GroupByPartition()
	total := 0
	totalSize := 0
	for partition, data := range partitions {
		pm := PartitionMetric{}
		for m, d := range data.GroupByMetric() {
			sort.Sort(d)
			value := d.Stored[d.Len()-1].Value
			switch m {
			case "LogStartOffset":
				pm.LogStartOffset = value
			case "LogEndOffset":
				pm.LogEndOffset = value
			case "Size":
				totalSize += value
				pm.Size = value
			case "BytesInPerSec":
				tm.BytesInPerSec = value
			case "BytesOutPerSec":
				tm.BytesOutPerSec = value
			case "MessagesInPerSec":
				tm.MessagesInPerSec = value
			}
		}
		if partition != "" {
			p, _ := zookeeper.Partition(name, partition)
			pm.Number = p.Number
			pm.Leader = p.Leader
			pm.ISR = p.Isr
			tm.PMs = append(tm.PMs, pm)
		}
		total += pm.LogEndOffset - pm.LogStartOffset
	}
	sort.Sort(tm.PMs)
	tm.MessageCount = total
	tm.Size = totalSize
	return tm
}
