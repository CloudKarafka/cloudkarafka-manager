package api

import (
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"net/http"
)

type consumerVM struct {
	Name   string            `json:"name"`
	Topics []consumedTopicVM `json:"topics"`
}

type consumedTopicVM struct {
	Name         string `json:"name"`
	ConsumeRatio int    `json:"consume_ratio"`
	Lag          int    `json:"lag"`
}

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
			consumedMessages := 0
			for _, off := range cps {
				consumedMessages += off.Offset
			}
			topic, _ := zookeeper.Topic(t)
			var parts []string
			for p, _ := range topic.Partitions {
				parts = append(parts, p)
			}
			topics = append(topics, consumedTopicVM{
				Name:         t,
				Lag:          consumedMessages - jmx.TopicMessageCount(t, parts),
				ConsumeRatio: int((len(cps) / len(topic.Partitions)) * 100),
			})
		}
	}
	writeJson(w, consumerVM{Name: vars["name"], Topics: topics})
}
