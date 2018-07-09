package dm

import (
	"cloudkarafka-mgmt/store"
	//"cloudkarafka-mgmt/zookeeper"
	"fmt"
	"sort"
	"strconv"
)

type ConsumerMetric struct {
	Name               string             `json:"name"`
	ConsumedPartitions consumedPartitions `json:"consumed_partitions"`
	ConsumedTopics     consumedTopics     `json:"consumed_topics"`
}

type consumedPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
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

func ConsumerMetrics(consumer string) ConsumerMetric {
	cm := ConsumerMetric{Name: consumer}
	cps := store.Intersection("consumer", consumer).GroupByTopic()
	//total := 0
	//totalSize := 0
	for topicName, data := range cps {
		t, err := baseTopic(T{Name: topicName})
		if err != nil {
			fmt.Println(err)
			continue
		}
		t = TopicMetrics(t)
		sort.Sort(data)
		tLag := 0
		for partition, d := range data.GroupByPartition() {
			pNr, err := strconv.Atoi(partition)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if len(t.Partitions) <= pNr {
				fmt.Println("Partition number", pNr)
				continue
			}
			sort.Sort(d)
			value := d.Last().Value
			p := t.Partitions[pNr]
			lag := p.LogEndOffset - value
			tLag += lag
			cm.ConsumedPartitions = append(cm.ConsumedPartitions, consumedPartition{
				Topic:     topicName,
				Partition: pNr,
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
