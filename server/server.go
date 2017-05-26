package server

import (
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func brokers(w http.ResponseWriter, r *http.Request) {
	brokers := zookeeper.Brokers()
	writeJson(w, brokers, nil)
}

func broker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	fmt.Fprintf(w, string(zookeeper.Broker(vars["id"])))
}

func topics(w http.ResponseWriter, r *http.Request) {
	topics, err := kafka.Topics()
	writeJson(w, topics, err)
}

func topic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic, err := kafka.Topic(vars["name"])
	writeJson(w, topic, err)
}

func consumers(w http.ResponseWriter, r *http.Request) {
	c, err := kafka.Consumers()
	writeJson(w, c, err)
}

func consumer(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	consumer, err := kafka.Consumer(vars["name"])
	writeJson(w, consumer, err)
}

func writeJson(w http.ResponseWriter, bytes interface{}, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, err.Error())
	} else {
		json.NewEncoder(w).Encode(bytes)
	}
}

func Start(port string) {
	r := mux.NewRouter()
	r.HandleFunc("/api/brokers", brokers)
	r.HandleFunc("/api/brokers/{id}", broker)
	r.HandleFunc("/api/topics", topics)
	r.HandleFunc("/api/topics/{name}", topic)
	r.HandleFunc("/api/consumers", consumers)
	r.HandleFunc("/api/consumers/{name}", consumer)
	http.Handle("/", r)
	s := &http.Server{
		Addr:         port,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	s.ListenAndServe()
}
