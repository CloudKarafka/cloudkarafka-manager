package api

import (
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

var client = http.Client{}

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
	bm, err = jmx.BrokerMetrics(vars["id"])

	if err != nil {
		req, err := http.NewRequest("get", fmt.Sprintf("http://%s:8080/api/brokers/%s/metrics", broker.Host, vars["id"]), nil)
		u, p, _ := r.BasicAuth()
		req.SetBasicAuth(u, p)
		res, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
		} else {
			dec := json.NewDecoder(res.Body)
			defer res.Body.Close()
			fmt.Println(dec.Decode(&bm))
		}
	}
	bvm = brokerVM{B: broker, BrokerMetric: bm}
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
