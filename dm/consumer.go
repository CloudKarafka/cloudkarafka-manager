package dm

import (
	"github.com/84codes/cloudkarafka-mgmt/store"

	"fmt"
	"sort"
)

type ConsumerMetric struct {
	Name               string             `json:"name"`
	ConsumedPartitions consumedPartitions `json:"consumed_partitions"`
	ConsumedTopics     consumedTopics     `json:"consumed_topics"`
}

type consumedPartition struct {
	Topic     string `json:"topic"`
	Partition string `json:"partition"`
	Lag       int    `json:"lag"`
}

type consumedTopic struct {
	Name     string `json:"name"`
	Coverage int    `json:"coverage"`
	Lag      int    `json:"lag"`
}

type consumedPartitions []consumedPartition
type consumedTopics []consumedTopic

func (pm consumedPartitions) Len() int      { return len(pm) }
func (pm consumedPartitions) Swap(i, j int) { pm[i], pm[j] = pm[j], pm[i] }
func (pm consumedPartitions) Less(i, j int) bool {
	in := pm[i]
	jn := pm[j]
	if in.Topic == jn.Topic {
		return in.Partition < jn.Partition
	} else {
		return in.Topic < jn.Topic
	}
}

func (pm consumedTopics) Len() int      { return len(pm) }
func (pm consumedTopics) Swap(i, j int) { pm[i], pm[j] = pm[j], pm[i] }
func (pm consumedTopics) Less(i, j int) bool {
	in := pm[i].Name
	jn := pm[j].Name
	return in < jn
}

func ConsumerMetrics(name string) ConsumerMetric {
	cm := ConsumerMetric{Name: name}
	consumer := store.Consumer(name)
	for topicName, partitions := range consumer {
		t, err := baseTopic(T{Name: topicName})
		if err != nil {
			fmt.Println(err)
			continue
		}
		t = TopicMetrics(t)
		tLag := 0
		for partition, ts := range partitions {
			var p Partition
			for _, p = range t.Partitions {
				if p.Number == partition {
					break
				} else {
					p = Partition{Number: "-1"}
				}
			}
			if p.Number == "-1" {
				fmt.Println("Partition number", partition)
				continue
			}
			lag := ts.Latest()
			tLag += lag
			cm.ConsumedPartitions = append(cm.ConsumedPartitions, consumedPartition{
				Topic:     topicName,
				Partition: partition,
				Lag:       lag,
			})
		}
		c := int(float64(len(cm.ConsumedPartitions)) / float64(len(t.Partitions)) * 100)
		cm.ConsumedTopics = append(cm.ConsumedTopics, consumedTopic{
			Name:     topicName,
			Lag:      tLag,
			Coverage: c,
		})
	}
	sort.Sort(cm.ConsumedTopics)
	sort.Sort(cm.ConsumedPartitions)
	return cm
}

func ConsumerTotalLagSeries(name, topic string) Series {
	c := store.Consumer(name)
	cps := c[topic]
	totalLag := make(store.Timeseries)
	for _, series := range cps {
		for ts, val := range series {
			totalLag.Add(val, ts)
		}
	}
	return throughputTimeseries(totalLag)
}
