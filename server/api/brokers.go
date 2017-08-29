package api

import (
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

type broker struct {
	KafkaVersion     string   `json:"kafka_version"`
	Version          int      `json:"-"`
	JmxPort          int      `json:"jmx_port"`
	Timestamp        string   `json:"timestamp"`
	Uptime           string   `json:"uptime"`
	Endpoints        []string `json:"endpoints"`
	Host             string   `json:"host"`
	Port             int      `json:"port"`
	Id               string   `json:"id"`
	BytesInPerSec    float64  `json:"bytes_in_per_sec"`
	BytesOutPerSec   float64  `json:"bytes_out_per_sec"`
	MessagesInPerSec float64  `json:"messages_in_per_sec"`
}

func Brokers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	brokers := zookeeper.Brokers()
	writeJson(w, brokers)
}

func Broker(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	b := broker{Id: vars["id"]}
	json.Unmarshal(zookeeper.Broker(vars["id"]), &b)
	ts, err := strconv.ParseInt(b.Timestamp, 10, 64)
	if err != nil {
		internalError(w, b)
	}
	t := time.Unix(ts/1000, 0)
	b.Uptime = time.Since(t).String()
	b.KafkaVersion, err = jmx.KafkaVersion(b.Id)
	if err != nil {
		internalError(w, b)
	}
	b.BytesOutPerSec, err = jmx.BrokerTopicMetric("BytesOutPerSec", "")
	if err != nil {
		internalError(w, b)
	}
	b.BytesInPerSec, err = jmx.BrokerTopicMetric("BytesInPerSec", "")
	if err != nil {
		internalError(w, b)
	}
	b.MessagesInPerSec, err = jmx.BrokerTopicMetric("MessagesInPerSec", "")
	if err != nil {
		internalError(w, b)
	}
	writeJson(w, b)
}
