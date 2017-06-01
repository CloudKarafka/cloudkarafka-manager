package handler

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		topics(w)
	case "POST":
		if err := r.ParseForm(); err != nil {
			internalError(w, err)
		} else {
			createTopic(w, r.Form)
		}
	}
}

func Topic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		topic(w, vars)
	default:
		http.NotFound(w, r)
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

func topic(w http.ResponseWriter, vars map[string]string) {
	topic, err := zookeeper.Topic(vars["topic"])
	if err != nil {
		internalError(w, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(topic))
	}
}

func topics(w http.ResponseWriter) {
	topics, err := zookeeper.Topics()
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, topics)
	}
}

func createTopic(w http.ResponseWriter, form url.Values) {
	partitions, err := strconv.Atoi(form.Get("partitions"))
	if err != nil {
		internalError(w, err)
		return
	}
	replicationFactor, err := strconv.Atoi(form.Get("replication_factor"))
	if err != nil {
		internalError(w, err)
		return
	}
	err = zookeeper.CreateTopic(form.Get("topic"), partitions, replicationFactor)
	if err != nil {
		internalError(w, err)
		return
	}
	topic, err := zookeeper.Topic(form.Get("topic"))
	if err != nil {
		internalError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(topic))
}
