package api

import (
	//"context"
	"encoding/json"
	"fmt"
	"net/http"

	//"time"

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
	//brokers := config.BrokerUrls
	var all []store.Metric
	/*
		conn, err := store.DialJMXServer()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		defer conn.Close()
		for _, m := range wanted {
			for brokerId, _ := range brokers {
				//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				//defer cancel()
				metrics, err := conn.GetMetrics(store.MetricRequest{
					brokerId,
					store.BeanFromString(m[0]),
					m[1],
				})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				all = append(all, metrics...)
			}
		}
	*/
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
