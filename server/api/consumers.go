package api

import (
	"context"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"goji.io/pat"
)

func ListConsumerGroups(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	res, err := store.FetchConsumerGroups(ctx)
	if err != nil {
		log.Error("api.list_consumers", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeAsJson(w, res)
}

type ConsumerGroup struct {
	Name    string                      `json:"name"`
	Topics  []string                    `json:"topics"`
	Lag     int                         `json:"lag"`
	Clients int                         `json:"consumers"`
	Members []store.ConsumerGroupMember `json:"members"`
}

func ViewConsumerGroup(w http.ResponseWriter, r *http.Request) {
	group := pat.Param(r, "name")
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	g, err := store.FetchConsumerGroups(ctx)
	if err != nil {
		log.Error("api.view_consumergroups", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumerGroup{
		Name:    group,
		Topics:  g.Topics(group),
		Lag:     g.Lag(group),
		Clients: g.NumberConsumers(group),
		Members: g[group],
	}
	writeAsJson(w, res)
}
