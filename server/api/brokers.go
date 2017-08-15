package api

import (
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"encoding/json"
	"net/http"
)

type broker struct {
	Version   string   `json:version`
	JmxPort   string   `json:jmx_port`
	Timestamp string   `json:timestamp`
	Endpoints []string `json:endpoints`
	Host      string   `json:host`
	Port      string   `json:port`
}

func Brokers(w http.ResponseWriter, r *http.Request) {
	brokers := zookeeper.Brokers()
	writeJson(w, brokers)
}

func Broker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	var b broker
	json.Unmarshal(zookeeper.Broker(vars["id"]), &b)
	b.Version = jmx.KafkaVersion()
	writeJson(w, b)
}
