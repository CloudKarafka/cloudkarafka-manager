package web

import (
	"context"
	"net/http"
	"sort"
	"strconv"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"goji.io/pat"
)

func Brokers(w http.ResponseWriter, r *http.Request) templates.Result {
	metricRequests := make([]store.MetricRequest, len(config.BrokerUrls)*3)
	i := 0
	for id, _ := range config.BrokerUrls {
		metricRequests[i] = store.MetricRequest{id, store.BeanBrokerBytesInPerSec, "OneMinuteRate"}
		metricRequests[i+1] = store.MetricRequest{id, store.BeanBrokerBytesOutPerSec, "OneMinuteRate"}
		metricRequests[i+2] = store.MetricRequest{id, store.BeanBrokerMessagesInPerSec, "OneMinuteRate"}
		i = i + 3
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	brokers, err := store.FetchBrokers(ctx, config.BrokerUrls.IDs(), metricRequests)
	if err != nil {
		log.Error("brokers", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].Broker.Id < brokers[j].Broker.Id
	})
	return templates.DefaultRenderer("brokers", brokers)
}

func Broker(w http.ResponseWriter, r *http.Request) templates.Result {
	id, err := strconv.Atoi(pat.Param(r, "id"))
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	metricRequests := make([]store.MetricRequest, len(config.BrokerUrls)*9)
	i := 0
	for id, _ := range config.BrokerUrls {
		metricRequests[i] = store.MetricRequest{id, store.BeanBrokerBytesInPerSec, "OneMinuteRate"}
		metricRequests[i+1] = store.MetricRequest{id, store.BeanBrokerBytesOutPerSec, "OneMinuteRate"}
		metricRequests[i+2] = store.MetricRequest{id, store.BeanBrokerMessagesInPerSec, "OneMinuteRate"}
		metricRequests[i+3] = store.MetricRequest{id, store.BeanBrokerBytesRejectedPerSec, "OneMinuteRate"}
		metricRequests[i+4] = store.MetricRequest{id, store.BeanBrokerLeaderCount, "Value"}
		metricRequests[i+5] = store.MetricRequest{id, store.BeanBrokerPartitionCount, "Value"}
		metricRequests[i+6] = store.MetricRequest{id, store.BeanBrokerOfflinePartitionsCount, "Value"}
		metricRequests[i+7] = store.MetricRequest{id, store.BeanBrokerUnderReplicatedPartitions, "Value"}
		metricRequests[i+8] = store.MetricRequest{id, store.BeanBrokerConnections, "connection-count"}
		i = i + 9
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	broker, err := store.FetchBroker(ctx, id, metricRequests)
	if err != nil {
		log.Error("broker", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	return templates.DefaultRenderer("broker", broker)
}
