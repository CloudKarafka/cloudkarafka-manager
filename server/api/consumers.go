package api

import (
	//"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"math"
	"net/http"
	//"strconv"
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
	groups := store.Store.GroupBy(func(d store.Data) string {
		return d.Id["group"]
	})
	var consumers []string
	for c, _ := range groups {
		consumers = append(consumers, c)
	}
	writeJson(w, consumers)
}

func Consumer(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	cts := store.Store.Select(func(d store.Data) bool {
		return d.Id["group"] == vars["name"]
	})
	if len(cts) == 0 {
		http.NotFound(w, r)
		return
	}
	var topics []consumedTopicVM
	groups := cts.GroupByTopic()
	for topic, data := range groups {
		if p.TopicRead(topic) {
			t, _ := zookeeper.Topic(topic)
			topics = append(topics, consumedTopicVM{
				Name:     topic,
				Coverage: int(math.Trunc(float64(len(data)) / float64(len(t.Partitions)) * 100)),
			})
		}
	}
	writeJson(w, consumerVM{Name: vars["name"], Topics: topics})
}

func ConsumerMetrics(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	lag := make(map[string]partitionLag)
	/*cts := kafka.Consumer(vars["name"], p)
	vars := mux.Vars(r)
	if cts == nil {
		http.NotFound(w, r)
		return
	}
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
	}*/
	writeJson(w, consumerMetric{Topics: lag})
}
