package api

import (
	"github.com/84codes/cloudkarafka-mgmt/dm"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"

	"github.com/dustin/go-humanize"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"

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

var (
	brokersMux = web.New()
)

func init() {
	brokersMux.Use(middleware.SubRouter)
	brokersMux.Use(func(c *web.C, h http.Handler) http.Handler {
		wrap := func(w http.ResponseWriter, r *http.Request) {
			p := permissions(*c)
			if !p.ClusterRead() {
				http.NotFound(w, r)
				return
			}
			h.ServeHTTP(w, r)
		}
		return http.HandlerFunc(wrap)
	})

	Mux.Get("/brokers", func(c web.C, w http.ResponseWriter, r *http.Request) {
		brokers, err := zookeeper.Brokers()
		if err != nil {
			internalError(w, err)
			return
		}
		WriteJson(w, brokers)
	})

	Mux.Get("/brokers/throughput", func(c web.C, w http.ResponseWriter, r *http.Request) {
		in := dm.AllBrokerBytesInPerSec()
		out := dm.AllBrokerBytesOutPerSec()
		WriteJson(w, map[string][]dm.DataPoint{"in": in, "out": out})
	})

	Mux.Get("/brokers/:id", func(c web.C, w http.ResponseWriter, r *http.Request) {
		broker, err := zookeeper.Broker(c.URLParams["id"])
		if err != nil {
			internalError(w, err)
			return
		}
		var (
			bvm brokerVM
			bm  dm.BrokerMetric
		)
		bm = dm.BrokerMetrics(c.URLParams["id"])
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
		WriteJson(w, bvm)
	})

	Mux.Get("/brokers/:id/jvm", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterRead() {
			http.NotFound(w, r)
			return
		}
		m := dm.BrokerJvmMetrics(c.URLParams["id"])
		WriteJson(w, m)
	})

	Mux.Get("/brokers/:id/health", func(c web.C, w http.ResponseWriter, r *http.Request) {
		m := dm.BrokerHealthMetrics(c.URLParams["id"])
		WriteJson(w, m)
	})

	Mux.Get("/brokers/:id/throughput", func(c web.C, w http.ResponseWriter, r *http.Request) {
		in := dm.BrokerBytesIn(c.URLParams["id"])
		out := dm.BrokerBytesOut(c.URLParams["id"])
		WriteJson(w, map[string][]dm.DataPoint{"in": in, "out": out})
	})
}
