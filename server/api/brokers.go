package api

import (
	"net/http"

	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"goji.io/pat"
)

type brokerVM struct {
	Id           int    `json:"id"`
	KafkaVersion string `json:"kafka_version"`
	Host         string `json:"host"`
	Controller   bool   `json:"controller"`
	Uptime       string `json:"uptime"`
	BytesIn      []int  `json:"bytes_in,omitempty"`
	BytesOut     []int  `json:"bytes_out,omitempty"`
	ISRShrink    []int  `json:"isr_shrink,omitempty"`
	ISRExpand    []int  `json:"isr_expand,omitempty"`
	Leader       int    `json:"leader"`
	Partitions   int    `json:"partitions"`
	TopicSize    string `json:"topic_size"`
}

func Brokers(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListBrokers() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	brokers := make([]brokerVM, len(store.Brokers()))
	for _, b := range store.Brokers() {
		brokers[b.Id] = brokerVM{
			Id:           b.Id,
			KafkaVersion: b.KafkaVersion,
			Host:         b.Host,
			Controller:   b.Controller,
			Uptime:       b.Uptime(),
		}
	}
	writeAsJson(w, brokers)
}

func Broker(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListBrokers() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	id := pat.Param(r, "id")
	b, ok := store.Broker(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	pc, lc, ts := store.BrokerToipcStats(b.Id)
	writeAsJson(w, brokerVM{
		Id:           b.Id,
		KafkaVersion: b.KafkaVersion,
		Host:         b.Host,
		Controller:   b.Controller,
		Uptime:       b.Uptime(),
		BytesIn:      b.BytesIn.Points,
		BytesOut:     b.BytesOut.Points,
		ISRExpand:    b.ISRExpand.Points,
		ISRShrink:    b.ISRShrink.Points,
		Partitions:   pc,
		Leader:       lc,
		TopicSize:    ts,
	})
}
