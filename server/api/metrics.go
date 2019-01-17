package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/metrics"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

func KafkaMetrics(w http.ResponseWriter, r *http.Request) {
	var wanted [][]string
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&wanted); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not parse request body: " + err.Error()))
		return
	}
	ch := make(chan []metrics.Metric)
	for _, m := range wanted {
		for brokerId, _ := range config.BrokerUrls {
			go metrics.QueryBrokerAsync(brokerId, m[0], m[1], ch)
		}
	}
	all := make([]metrics.Metric, 0)
	for i := 0; i < len(wanted)*len(config.BrokerUrls); i++ {
		all = append(all, <-ch...)
	}
	close(ch)
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
