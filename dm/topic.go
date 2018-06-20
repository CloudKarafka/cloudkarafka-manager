package dm

import (
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"

	"sort"
	"strconv"
)

type T struct {
	Name              string                 `json:"name,omitempty"`
	Config            map[string]interface{} `json:"config"`
	PartitionCount    int                    `json:"partition_count,1"`
	ReplicationFactor int                    `json:"replication_factor,1"`
	BrokerSpread      int                    `json:"broker_spread"`
	BytesInPerSec     int                    `json:"bytes_in_per_sec"`
	BytesOutPerSec    int                    `json:"bytes_out_per_sec"`
	MessagesInPerSec  int                    `json:"messages_in_per_sec"`
	MessageCount      int                    `json:"message_count"`
	Size              int                    `json:"size"`
	Partitions        Partitions             `json:"partitions"`
}

type Partition struct {
	Number         string `json:"number"`
	Leader         int    `json:"leader"`
	Replicas       []int  `json:"replicas"`
	ISR            []int  `json:"isr"`
	LogStartOffset int    `json:"log_start_offset"`
	LogEndOffset   int    `json:"log_end_offset"`
	Size           int    `json:"size"`
}

type Partitions []Partition

func (pm Partitions) Len() int      { return len(pm) }
func (pm Partitions) Swap(i, j int) { pm[i], pm[j] = pm[j], pm[i] }
func (pm Partitions) Less(i, j int) bool {
	in, _ := strconv.Atoi(pm[i].Number)
	jn, _ := strconv.Atoi(pm[j].Number)
	return in < jn
}

func Topic(name string) (T, error) {
	var err error
	t := T{Name: name}
	t, err = baseTopic(t)
	if err != nil {
		return t, err
	}
	t = TopicMetrics(t)
	return t, nil
}

func baseTopic(t T) (T, error) {
	zkT, err := zookeeper.Topic(t.Name)
	if err != nil {
		return t, err
	}
	t.Config = zkT.Config
	t.PartitionCount = len(zkT.Partitions)
	t.ReplicationFactor = len(zkT.Partitions["0"])
	t.Partitions = make(Partitions, t.PartitionCount)
	for num, replicas := range zkT.Partitions {
		p, _ := zookeeper.Partition(t.Name, num)
		n, _ := strconv.Atoi(num)
		t.Partitions[n] = Partition{
			Number:   num,
			Leader:   p.Leader,
			Replicas: replicas,
			ISR:      p.Isr,
		}
	}
	return t, nil
}

func partitionMetric(p Partition, data store.Store) Partition {
	for m, d := range data.GroupByMetric() {
		sort.Sort(d)
		value := d.Last().Value
		switch m {
		case "LogStartOffset":
			p.LogStartOffset = value
		case "LogEndOffset":
			p.LogEndOffset = value
		case "Size":
			p.Size = value
		}
	}
	return p
}

func TopicMetrics(t T) T {
	partitions := store.SelectWithIndex(t.Name).GroupByPartition()
	for partition, data := range partitions {
		if partition != "" {
			if num, _ := strconv.Atoi(partition); num < len(t.Partitions) {
				t.Partitions[num] = partitionMetric(t.Partitions[num], data)
			}
		} else {
			for m, d := range data.GroupByMetric() {
				sort.Sort(d)
				value := d.Last().Value
				switch m {
				case "BytesInPerSec":
					t.BytesInPerSec = value
				case "BytesOutPerSec":
					t.BytesOutPerSec = value
				case "MessagesInPerSec":
					t.MessagesInPerSec = value
				}
			}
		}
	}
	total := 0
	totalSize := 0
	for _, p := range t.Partitions {
		total += p.LogEndOffset - p.LogStartOffset
		totalSize += p.Size
	}
	sort.Sort(t.Partitions)
	t.MessageCount = total
	t.Size = totalSize
	return t
}
