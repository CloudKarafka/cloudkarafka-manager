package api

import (
	"cloudkarafka-mgmt/config"
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/zookeeper"

	"encoding/json"
	"fmt"
	"net/http"
)

var client = http.Client{}

func internalError(w http.ResponseWriter, bytes interface{}) {
	fmt.Println(bytes)
	w.WriteHeader(http.StatusInternalServerError)
	writeJson(w, bytes)
}

func writeJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bytes)
}

func fetchRemote(path string, r *http.Request, out interface{}) error {
	req, err := http.NewRequest("get", path, nil)
	u, p, _ := r.BasicAuth()
	req.SetBasicAuth(u, p)
	res, err := client.Do(req)
	if err != nil {
		return err
	} else {
		dec := json.NewDecoder(res.Body)
		defer res.Body.Close()
		return dec.Decode(&out)
	}
}

func fetchOffsetMetric(topic, partition string, r *http.Request) (jmx.OffsetMetric, error) {
	var om jmx.OffsetMetric
	broker, _ := zookeeper.LeaderFor(topic, partition)
	path := fmt.Sprintf("http://%s:%s/api/topics/%s/%s/metrics", broker.Host, config.Port, topic, partition)
	err := fetchRemote(path, r, &om)
	return om, err
}
