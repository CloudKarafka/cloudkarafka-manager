package api

import (
	"net/http"

	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"goji.io/pat"
)

func Brokers(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListBrokers() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	writeAsJson(w, store.Brokers())
}

func Broker(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListBrokers() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	id := pat.Param(r, "id")
	broker, ok := store.Broker(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeAsJson(w, broker)
}
