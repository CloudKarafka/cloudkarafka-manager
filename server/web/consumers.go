package web

import (
	"context"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"goji.io/pat"
)

func ListConsumerGroups(w http.ResponseWriter, r *http.Request) templates.Result {
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	res, err := store.FetchConsumerGroups(ctx)
	if err != nil {
		log.Error("list_consumergroups", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	return templates.DefaultRenderer("consumer_groups", res)
}

type ConsumerGroup struct {
	Name    string
	Topics  []string
	Lag     int
	Clients int
	Members []store.ConsumerGroupMember
}

func ViewConsumerGroup(w http.ResponseWriter, r *http.Request) templates.Result {
	group := pat.Param(r, "name")
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	g, err := store.FetchConsumerGroups(ctx)
	if err != nil {
		log.Error("list_consumergroups", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	res := ConsumerGroup{
		Name:    group,
		Topics:  g.Topics(group),
		Lag:     g.Lag(group),
		Clients: g.NumberConsumers(group),
		Members: g[group],
	}
	return templates.DefaultRenderer("consumer_group", res)
}
