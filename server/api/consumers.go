package api

import (
	//"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/dm"
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"net/http"
)

type consumerVM struct {
	Name         string            `json:"name"`
	Topics       []consumedTopicVM `json:"topics"`
	PartitionLag []partitionLag    `json:"partition_lag"`
}

type consumedTopicVM struct {
	Name     string `json:"name"`
	Coverage int    `json:"coverage"`
}

type consumerMetric struct {
	Topics map[string]partitionLag `json:"topics"`
}

type partitionLag struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Lag       int    `json:"lag"`
}

func Consumers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	consumers := store.IndexedNames("group")
	writeJson(w, consumers)
}

func Consumer(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	data := dm.ConsumerMetrics(vars["name"])
	writeJson(w, data)
}

/*
	var topics []consumedTopicVM
	for topic, data := range cts {
		partitions := data.GroupByPartition()
		if p.TopicRead(topic) {
			t, _ := zookeeper.Topic(topic)
			pl := partitionsLag(topic, p)
			c := int(math.Trunc(float64(len(partitions)) / float64(len(pl)) * 100))
			topics = append(topics, consumedTopicVM{
				Name:         topic,
				Coverage:     c,
				PartitionLag: pl,
			})
		}
	}
	writeJson(w, consumerVM{Name: vars["name"], Topics: topics})
}

func partitionsLag(topic string, p zookeeper.Permissions) []partitionLag {
	var lags []partitionLag
	cts := store.SelectWithIndex(topic).GroupByTopic()
	for t, topics := range cts {
		if p.TopicRead(t) {
			partitions := topics.GroupByPartition()
			for part, _ := range partitions {
				lag := om.LogEndOffset - off.Value
				lags = append(lags, partitionLag{Topic: topic, Partition: part, Lag: lag})
			}
		}
	}
	return lags
}*/
