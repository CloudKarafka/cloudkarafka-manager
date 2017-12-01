package api

import (
	"cloudkarafka-mgmt/config"
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"fmt"
	"net/http"
	"strconv"
	"time"
)

type brokerVM struct {
	zookeeper.B
	jmx.BrokerMetric
	Uptime string `json:"uptime"`
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
	if err != nil {
		return
	}
	var (
		bm  jmx.BrokerMetric
		bvm brokerVM
	)
	if vars["id"] == jmx.BrokerId {
		bm, err = jmx.BrokerMetrics(vars["id"])
	} else {
		path := fmt.Sprintf("http://%s:%s/api/brokers/%s/metrics", broker.Host, config.Port, vars["id"])
		err = fetchRemote(path, r, &bm)
	}
	if err != nil {
		bvm = brokerVM{B: broker}
	} else {
		bvm = brokerVM{B: broker, BrokerMetric: bm}
	}
	ts, err := strconv.ParseInt(bvm.Timestamp, 10, 64)
	if err != nil {
		internalError(w, bvm)
		return
	}
	t := time.Unix(ts/1000, 0)
	bvm.Uptime = time.Since(t).String()
	writeJson(w, bvm)
}

func BrokerMetrics(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	bm, err := jmx.BrokerMetrics(vars["id"])
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, bm)
	}
}
