package api

import (
	"cloudkarafka-mgmt/dm"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"

	"net/http"
	"strconv"
	"strings"
	"time"
)

type brokerVM struct {
	dm.BrokerMetric
	zookeeper.B

	Uptime string `json:"uptime"`
}

func Brokers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	brokers, err := zookeeper.Brokers()
	if err != nil {
		internalError(w, err)
		return
	}
	if p.ClusterRead() {
		writeJson(w, brokers)
	}
}

func Broker(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	broker, err := zookeeper.Broker(vars["id"])
	if err != nil {
		return
	}
	var (
		bvm brokerVM
		bm  dm.BrokerMetric
	)
	bm = dm.BrokerMetrics(vars["id"])
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
	bvm.Uptime = strings.TrimSpace(humanize.RelTime(time.Now(), t, "", ""))
	writeJson(w, bvm)
}

func BrokerThroughputTimeseries(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	in := dm.BrokerBytesIn(vars["id"])
	out := dm.BrokerBytesOut(vars["id"])
	writeJson(w, map[string][]dm.DataPoint{"in": in, "out": out})
}

func AllBrokerThroughputTimeseries(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	in := dm.AllBrokerBytesInPerSec()
	out := dm.AllBrokerBytesOutPerSec()
	writeJson(w, map[string][]dm.DataPoint{"in": in, "out": out})
}
