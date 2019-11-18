package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

func KafkaMetrics(w http.ResponseWriter, r *http.Request) {
	var wanted [][]string
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&wanted); err != nil {
		http.Error(w, "Could not parse request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	brokers := config.BrokerUrls
	l := len(wanted) * len(brokers)
	metricReqs := make([]store.MetricRequest, l)
	i := 0
	for _, m := range wanted {
		for brokerId, _ := range brokers {
			metricReqs[i] = store.MetricRequest{
				brokerId,
				store.BeanFromString(m[0]),
				m[1],
			}
		}
	}
	ch := store.GetMetricsAsync(metricReqs)
	all := make([]store.Metric, 0)
	for i := 0; i < l; i++ {
		r := <-ch
		if r.Error != nil {
			http.Error(w, r.Error.Error(), http.StatusInternalServerError)
			return
		}
		all = append(all, r.Metrics...)
	}
	writeAsJson(w, all)
}

func ZookeeperMetrics(w http.ResponseWriter, r *http.Request) {
	res := make(map[int]map[string]interface{})
	for id, hp := range config.BrokerUrls {
		uri := fmt.Sprintf("%s:2181", hp.Host)
		res[id] = zookeeper.Stats(uri)
		for k, v := range zookeeper.Metrics(uri) {
			res[id][k] = v
		}
	}
	writeAsJson(w, res)
}
