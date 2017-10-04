package api

import (
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"fmt"
	"math"
	"net/http"
	"strconv"
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

type partitionLag map[int]int

func Consumers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	writeJson(w, kafka.Consumers(p))
}

func Consumer(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	cts := kafka.Consumer(vars["name"], p)
	if cts == nil {
		http.NotFound(w, r)
		return
	}
	var topics []consumedTopicVM
	for t, cps := range cts {
		if p.TopicRead(t) {
			topic, _ := zookeeper.Topic(t)
			topics = append(topics, consumedTopicVM{
				Name:     t,
				Coverage: int(math.Trunc(float64(len(cps)) / float64(len(topic.Partitions)) * 100)),
			})
		}
	}
	writeJson(w, consumerVM{Name: vars["name"], Topics: topics})
}

func ConsumerMetrics(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	cts := kafka.Consumer(vars["name"], p)
	if cts == nil {
		http.NotFound(w, r)
		return
	}
	lag := make(map[string]partitionLag)
	for t, cps := range cts {
		if p.TopicRead(t) {
			pl := make(partitionLag)
			for part, off := range cps {
				om, err := fetchOffsetMetric(t, strconv.Itoa(part), r)
				if err != nil {
					fmt.Println("[ERROR]", err)
				}
				pl[part] = om.LogEndOffset - off.Offset
			}
			lag[t] = pl
		}
	}
	writeJson(w, consumerMetric{Topics: lag})
}
