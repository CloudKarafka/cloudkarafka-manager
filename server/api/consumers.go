package api

import (
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"fmt"
	"net/http"
)

type consumerView struct {
	Name   string   `json:"name"`
	Topics []string `json:"topics"`
}

func Consumers(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	writeJson(w, kafka.Consumers(p))
}

func Consumer(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	consumer := kafka.Consumer(vars["name"], p)
	fmt.Println(consumer)
	if consumer == nil {
		http.NotFound(w, r)
		return
	}
	var topics []string
	for t, _ := range consumer {
		if p.TopicRead(t) {
			topics = append(topics, t)
		}
	}
	writeJson(w, consumerView{Name: vars["name"], Topics: topics})
}
