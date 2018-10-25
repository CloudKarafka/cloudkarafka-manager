package api

import (
	"github.com/84codes/cloudkarafka-mgmt/dm"
	"github.com/84codes/cloudkarafka-mgmt/store"

	"github.com/zenazn/goji/web"

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

var (
	consumerMux = web.New()
)

func init() {
	Mux.Get("/consumers", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		allConsumers := store.Consumers()
		var myConsumers []string
		for _, c := range allConsumers {
			if p.ConsumerRead(c) {
				myConsumers = append(myConsumers, c)
			}
		}
		WriteJson(w, myConsumers)
	})

	Mux.Get("/consumers/:name", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ConsumerRead(c.URLParams["name"]) {
			http.NotFound(w, r)
			return
		}
		WriteJson(w, dm.ConsumerMetrics(c.URLParams["name"]))
	})

	Mux.Get("/consumers/:name/:topic/lag", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ConsumerRead(c.URLParams["name"]) && p.TopicRead(c.URLParams["topic"]) {
			http.NotFound(w, r)
			return
		}
		WriteJson(w, dm.ConsumerTotalLagSeries(c.URLParams["name"], c.URLParams["topic"]))
	})
}
