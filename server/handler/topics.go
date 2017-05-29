package handler

import (
	"cloudkarafka-mgmt/kafka"
	"github.com/gorilla/mux"

	"net/http"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	topics, err := kafka.Topics()
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, topics)
	}
}

func Topic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic, err := kafka.Topic(vars["name"])
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, topic)
	}
}
