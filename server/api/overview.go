package api

import (
	"net/http"

	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
)

type overviewVM struct {
	Version      string `json:"version"`
	Uptime       string `json:"uptime"`
	Brokers      int    `json:"brokers"`
	Topics       int    `json:"topics"`
	Partitions   int    `json:"partitions"`
	TopicSize    string `json:"topic_size"`
	Messages     int    `json:"messages"`
	Consumers    int    `json:"consumers"`
	MessageRates []int  `json:"message_rates"`
	BytesOut     []int  `json:"bytes_out"`
	BytesIn      []int  `json:"bytes_in"`
	ISRExpand    []int  `json:"isr_expand"`
	ISRShrink    []int  `json:"isr_shrink"`
}

func Overview(w http.ResponseWriter, r *http.Request) {
	var (
		user      = r.Context().Value("user").(mw.SessionUser)
		brokers   = store.DB.Brokers()
		topics    = topics(user.Permissions.DescribeTopic)
		consumers = store.DB.Consumers()
	)
	writeAsJson(w, overviewVM{
		Version:    config.Version,
		Uptime:     store.Uptime(),
		Brokers:    len(brokers),
		Topics:     len(topics),
		Consumers:  len(consumers),
		Partitions: store.Partitions(),
		TopicSize:  store.TotalTopicSize(),
		Messages:   store.TotalMessageCount(),
		BytesOut:   store.DB.SumBrokerSeries("bytes_out").All(),
		BytesIn:    store.DB.SumBrokerSeries("bytes_in").All(),
		ISRShrink:  store.DB.SumBrokerSeries("isr_shrink").All(),
		ISRExpand:  store.DB.SumBrokerSeries("isr_expand").All(),
	})
}
