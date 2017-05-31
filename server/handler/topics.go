package handler

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"fmt"
	"net/http"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	topics, err := zookeeper.Topics()
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, topics)
	}
}

func Topic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic, err := zookeeper.Topic(vars["topic"])
	if err != nil {
		internalError(w, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(topic))
	}
}

func Partition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	partition, err := zookeeper.Partition(vars["topic"], vars["partition"])
	if err != nil {
		internalError(w, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(partition))
	}
}
