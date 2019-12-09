package api

import (
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
)

type overviewVM struct {
	Version      string        `json:"version"`
	Uptime       string        `json:"uptime"`
	Brokers      int           `json:"brokers"`
	Topics       int           `json:"topics"`
	Partitions   int           `json:"partitions"`
	TopicSize    int           `json:"topic_size"`
	Messages     int           `json:"messages"`
	Consumers    int           `json:"consumers"`
	DataRates    []int         `json:"data_rates"`
	MessageRates []int         `json:"message_rates"`
	BytesOut     []store.Point `json:"bytes_out"`
	BytesIn      []store.Point `json:"bytes_in"`
}

func Overview(w http.ResponseWriter, r *http.Request) {
	vm := overviewVM{
		Version:    config.Version,
		Uptime:     store.Uptime(),
		Brokers:    store.Brokers(),
		Topics:     store.Topics(),
		Consumers:  store.Consumers(),
		Partitions: store.Partitions(),
		TopicSize:  store.TotalTopicSize(),
		Messages:   store.TotalMessageCount(),
		BytesOut:   store.BrokerTotal("BytesOutPerSec").All(),
		BytesIn:    store.BrokerTotal("BytesInPerSec").All(),
	}
	writeAsJson(w, vm)
}
