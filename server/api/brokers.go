package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
	m "github.com/84codes/cloudkarafka-mgmt/metrics"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	"goji.io/pat"
)

var brokerMetrics = map[string]string{
	"messages_MessagesInPerSec": "Messages in",
	"bytes_BytesInPerSec":       "Bytes in",
	"bytes_BytesOutPerSec":      "Bytes out",
	"bytes_BytesRejectedPerSec": "Bytes rejected",
}

func Brokers(w http.ResponseWriter, r *http.Request) {
	i := 0
	res := make([]map[string]interface{}, len(config.BrokerUrls))
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	for brokerId, _ := range config.BrokerUrls {
		broker, err := m.FetchBroker(brokerId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] Brokers: could not get broker info from zk: %s", err)
			continue
		}
		res[i] = map[string]interface{}{
			"details": broker,
		}
		m, err := m.FetchBrokerMetrics(ctx, brokerId, false)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] Brokers: failed fetching broker(%d) metrics: %s\n", brokerId, err)
		} else {
			res[i]["metrics"] = m
		}
		i += 1
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i]["details"].(m.Broker).Id < res[j]["details"].(m.Broker).Id
	})
	writeAsJson(w, res)
}

func Broker(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(pat.Param(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Broker id must a an integer"))
		return
	}
	broker, err := m.FetchBroker(id)
	if err != nil {
		if err == zookeeper.PathDoesNotExistsErr {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	res := map[string]interface{}{
		"details": broker,
	}
	conn, err := m.FetchBrokerConnections(id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] Broker: failed fetching connections for %d: %s\n", id, err)
	} else {
		res["connections"] = conn
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	d, err := m.FetchBrokerMetrics(ctx, id, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] Broker: failed fetching broker(%d) metrics: %s\n", id, err)
	} else {
		res["metrics"] = d
	}

	writeAsJson(w, res)
}

func BrokersThroughput(w http.ResponseWriter, r *http.Request) {
	from := time.Now().Add(time.Hour * 2 * -1)
	brokerIds := make([]int, len(config.BrokerUrls))
	i := 0
	for id, _ := range config.BrokerUrls {
		brokerIds[i] = id
		i += 1
	}
	res, err := m.BrokersThroughput(brokerMetrics, brokerIds, from)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	writeAsJson(w, res)
}
func BrokerThroughput(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(pat.Param(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Broker id must a an integer"))
		return
	}
	from := time.Now().Add(time.Hour * 2 * -1)
	res, err := m.BrokersThroughput(brokerMetrics, []int{id}, from)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	writeAsJson(w, res)

}
