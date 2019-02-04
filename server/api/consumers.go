package api

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	m "github.com/cloudkarafka/cloudkarafka-manager/metrics"
	"goji.io/pat"
)

func Consumers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	res, err := m.FetchConsumerGroups(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] api.Consumers: %s\n", err)
		http.Error(w, "Could not fetch consumers", http.StatusInternalServerError)
		return
	}
	writeAsJson(w, res)
}

func Consumer(w http.ResponseWriter, r *http.Request) {
	group := pat.Param(r, "name")
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	g, err := m.FetchConsumerGroups(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] api.Consumer: %s\n", err)
		http.Error(w, "Could not fetch consumer "+group, http.StatusInternalServerError)
		return
	}
	res := map[string]interface{}{
		"details": map[string]interface{}{
			"topics":  g.Topics(group),
			"lag":     g.Lag(group),
			"clients": g.NumberConsumers(group),
		},
		"members": g[group],
	}
	writeAsJson(w, res)
}
