package api

import (
	"net/http"
	"sort"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"goji.io/pat"
)

func ListConsumerGroups(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListGroups() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if config.NoConsumers {
		writeAsJson(w, Page(1, 1, store.ConsumerSlice{}))
		return
	}
	ps, p, err := pageInfo(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	consumers := consumers(user.Permissions.DescribeGroup)
	sort.Slice(consumers, func(i, j int) bool { return consumers[i].Name < consumers[j].Name })
	writeAsJson(w, Page(ps, p, consumers))
}

func ViewConsumerGroup(w http.ResponseWriter, r *http.Request) {
	group := pat.Param(r, "name")
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.DescribeGroup(group) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	g, ok := store.Consumer(group)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeAsJson(w, g)
}
