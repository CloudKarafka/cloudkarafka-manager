package web

import (
	"context"
	"net/http"
	"sort"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	m "github.com/cloudkarafka/cloudkarafka-manager/metrics"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
)

func Brokers(w http.ResponseWriter, r *http.Request) templates.Result {
	res := make([]map[string]interface{}, 0)
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	for brokerId, _ := range config.BrokerUrls {
		broker, err := m.FetchBroker(brokerId)
		if err != nil {
			log.Warn("fetch_brokers", log.ErrorEntry{err})
			return templates.ErrorRenderer(err)
		}
		b := map[string]interface{}{
			"details": broker,
		}
		m, err := m.FetchBrokerMetrics(ctx, brokerId, false)
		if err != nil {
			log.Info("fetch_broker_metrics", log.ErrorEntry{err})
		} else {
			b["metrics"] = m
		}
		res = append(res, b)

	}
	sort.Slice(res, func(i, j int) bool {
		return res[i]["details"].(m.Broker).Id < res[j]["details"].(m.Broker).Id
	})
	return templates.DefaultRenderer("brokers", res)
}

func Broker(w http.ResponseWriter, r *http.Request) templates.Result {
	res := ""
	return templates.DefaultRenderer("broker", res)
}
