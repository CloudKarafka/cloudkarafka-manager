package api

import (
	//"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"math"
	"net/http"
)

type consumerVM struct {
	Name   string            `json:"name"`
	Topics []consumedTopicVM `json:"topics"`
}

type consumedTopicVM struct {
	Name     string `json:"name"`
	Coverage int    `json:"coverage"`
}

type consumerMetric struct {
	Topics map[string]partitionLag `json:"topics"`
}

type partitionLag map[string]int

func Consumers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	consumers := store.IndexedNames("group")
	writeJson(w, consumers)
}

func Consumer(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	cts := store.SelectWithIndex(vars["name"]).GroupByTopic()
	if len(cts) == 0 {
		http.NotFound(w, r)
		return
	}
	var topics []consumedTopicVM
	for topic, data := range cts {
		partitions := data.GroupByPartition()
		if p.TopicRead(topic) {
			t, _ := zookeeper.Topic(topic)
			topics = append(topics, consumedTopicVM{
				Name:     topic,
				Coverage: int(math.Trunc(float64(len(partitions)) / float64(len(t.Partitions)) * 100)),
			})
		}
	}
	writeJson(w, consumerVM{Name: vars["name"], Topics: topics})
}

func ConsumerMetrics(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	lag := make(map[string]partitionLag)
	vars := mux.Vars(r)
	cts := store.SelectWithIndex(vars["name"]).GroupByTopic()
	if cts == nil {
		http.NotFound(w, r)
		return
	}
	for t, topics := range cts {
		if p.TopicRead(t) {
			partitions := topics.GroupByPartition()
			pl := make(partitionLag)
			for part, _ := range partitions {
				pl[part] = -1
				//pl[part] = om.LogEndOffset - off.Value
			}
			lag[t] = pl
		}
	}
	writeJson(w, consumerMetric{Topics: lag})
}
