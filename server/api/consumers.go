package api

import (
	//"context"
	"net/http"

	//"github.com/cloudkarafka/cloudkarafka-manager/config"
	//"github.com/cloudkarafka/cloudkarafka-manager/log"
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
	writeAsJson(w, store.Consumers())
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
	//res := ConsumerGroup{
	//Name:    group,
	//Topics:  g.Topics(group),
	//Lag:     g.Lag(group),
	//Clients: g.NumberConsumers(group),
	//Members: g[group],
	//}
	writeAsJson(w, g)
}
