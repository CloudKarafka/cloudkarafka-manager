package api

import (
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"net/http"
	"strconv"
	"time"
)

type brokerVM struct {
	zookeeper.B
	KafkaVersion     string  `json:"kafka_version"`
	Uptime           string  `json:"uptime"`
	BytesInPerSec    float64 `json:"bytes_in_per_sec"`
	BytesOutPerSec   float64 `json:"bytes_out_per_sec"`
	MessagesInPerSec float64 `json:"messages_in_per_sec"`
}

func Brokers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	brokers, err := zookeeper.Brokers()
	if err != nil {
		internalError(w, err)
		return
	}
	writeJson(w, brokers)
}

func Broker(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	broker, err := zookeeper.Broker(vars["id"])
	b := brokerVM{B: broker}

	ts, err := strconv.ParseInt(b.Timestamp, 10, 64)
	if err != nil {
		internalError(w, b)
		return
	}
	t := time.Unix(ts/1000, 0)
	b.Uptime = time.Since(t).String()
	b.KafkaVersion, err = jmx.KafkaVersion(b.Id)
	if err != nil {
		internalError(w, b)
		return
	}
	b.BytesOutPerSec, err = jmx.BrokerTopicMetric("BytesOutPerSec", "")
	if err != nil {
		internalError(w, b)
		return
	}
	b.BytesInPerSec, err = jmx.BrokerTopicMetric("BytesInPerSec", "")
	if err != nil {
		internalError(w, b)
		return
	}
	b.MessagesInPerSec, err = jmx.BrokerTopicMetric("MessagesInPerSec", "")
	if err != nil {
		internalError(w, b)
		return
	}
	writeJson(w, b)
}
