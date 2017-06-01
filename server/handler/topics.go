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
		topic(w, vars["topic"])
	case "PUT":
		if err := r.ParseForm(); err != nil {
			internalError(w, err)
		} else {
			updateTopic(w, vars["topic"], r.Form)
		}
	case "DELETE":
		deleteTopic(w, vars["topic"])
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

func topic(w http.ResponseWriter, name string) {
	topic, err := zookeeper.Topic(name)
	if err != nil {
		internalError(w, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(topic))
	}
}

func deleteTopic(w http.ResponseWriter, topic string) {
	err := zookeeper.DeleteTopic(topic)
	if err != nil {
		internalError(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
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
	partitions, replicationFactor, err := parseTopicOptions(form)
	if err != nil {
		internalError(w, err)
		return
	}
	err = zookeeper.CreateOrUpdateTopic(form.Get("topic"), partitions, replicationFactor)
	if err != nil {
		internalError(w, err)
		return
	}
	topic(w, form.Get("topic"))
}

func updateTopic(w http.ResponseWriter, name string, form url.Values) {
	partitions, replicationFactor, err := parseTopicOptions(form)
	if err != nil {
		internalError(w, err)
		return
	}
	err = zookeeper.CreateOrUpdateTopic(name, partitions, replicationFactor)
	if err != nil {
		internalError(w, err)
		return
	}
	topic(w, name)
}

func parseTopicOptions(form url.Values) (int, int, error) {
	partitions, err := strconv.Atoi(form.Get("partitions"))
	if err != nil {
		return 0, 0, err
	}
	replicationFactor, err := strconv.Atoi(form.Get("replication_factor"))
	if err != nil {
		return 0, 0, err
	}
	return partitions, replicationFactor, nil
}
