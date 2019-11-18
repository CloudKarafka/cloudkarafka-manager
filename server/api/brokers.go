package api

import (
	"context"
	"net/http"
	"sort"
	"strconv"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"goji.io/pat"
)

func Brokers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	brokers, err := store.FetchBrokers(ctx, config.BrokerUrls.IDs(), nil)
	if err != nil {
		log.Error("brokers", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ts := make([]store.Broker, len(brokers))
	i := 0
	for _, t := range brokers {
		if t.Error != nil {
			log.Error("api.list_brokers", log.ErrorEntry{err})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else {
			ts[i] = t.Broker
			i += 1
		}
	}
	sort.Slice(ts, func(i, j int) bool {
		return ts[i].Id < ts[j].Id
	})
	writeAsJson(w, ts)
}

func Broker(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(pat.Param(r, "id"))
	if err != nil {
		http.Error(w, "Broker id must a an integer", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	broker, err := store.FetchBroker(ctx, id, nil)
	if err != nil {
		log.Error("brokers", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeAsJson(w, broker)
}
