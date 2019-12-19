package api

import (
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
)

type overviewVM struct {
	Version      string `json:"version"`
	Uptime       string `json:"uptime"`
	Brokers      int    `json:"brokers"`
	Topics       int    `json:"topics"`
	Partitions   int    `json:"partitions"`
	TopicSize    int    `json:"topic_size"`
	Messages     int    `json:"messages"`
	Consumers    int    `json:"consumers"`
	DataRates    []int  `json:"data_rates"`
	MessageRates []int  `json:"message_rates"`
	BytesOut     []int  `json:"bytes_out"`
	BytesIn      []int  `json:"bytes_in"`
}

func Overview(w http.ResponseWriter, r *http.Request) {
	writeAsJson(w, overviewVM{
		Version:    config.Version,
		Uptime:     store.Uptime(),
		Brokers:    len(store.Brokers()),
		Topics:     len(store.Topics()),
		Consumers:  len(store.Consumers()),
		Partitions: store.Partitions(),
		TopicSize:  store.TotalTopicSize(),
		Messages:   store.TotalMessageCount(),
		BytesOut:   store.SumBrokerSeries("bytes_out").All(),
		BytesIn:    store.SumBrokerSeries("bytes_in").All(),
	})
}
