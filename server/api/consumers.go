package api

import (
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"net/http"
)

func Consumers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	c, err := kafka.Consumers()
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, c)
	}
}

func Consumer(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	consumer, err := kafka.Consumer(vars["name"])
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, consumer)
	}
}
