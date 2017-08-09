package api

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"fmt"
	"net/http"
)

func Brokers(w http.ResponseWriter, r *http.Request) {
	brokers := zookeeper.Brokers()
	writeJson(w, brokers)
}

func Broker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	fmt.Fprintf(w, string(zookeeper.Broker(vars["id"])))
}
