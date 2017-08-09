package api

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

var (
	replicationFactorRequired = errors.New("ERROR: must suply replication_factor and it must be numeric.")
	partitionsRequired        = errors.New("ERROR: must suply partitions and it must be numeric")
)

type topicData struct {
	Name              string                 `json:"name,omitempty"`
	Partitions        int                    `json:"partitions,1"`
	ReplicationFactor int                    `json:"replication_factor,1"`
	Config            map[string]interface{} `json:"config"`
}

func Topics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		topics(w)
	case "POST":
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err)
		} else {
			createTopic(w, t)
		}
	}
}

func Topic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		topic(w, vars["topic"])
	case "PUT":
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err)
		} else {
			updateTopic(w, vars["topic"], t)
		}
	case "DELETE":
		deleteTopic(w, vars["topic"])
	default:
		http.NotFound(w, r)
	}
}

func Config(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cfg, err := zookeeper.Config(vars["topic"])
	if err != nil {
		internalError(w, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(cfg))
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

func decodeTopic(r *http.Request) (topicData, error) {
	var (
		t   topicData
		err error
	)
	switch r.Header.Get("content-type") {
	case "application/json":
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&t)
		defer r.Body.Close()
	default:
		err = r.ParseForm()
		t.Name = r.PostForm.Get("name")
		t.Partitions, err = strconv.Atoi(r.PostForm.Get("partitions"))
		t.ReplicationFactor, err = strconv.Atoi(r.PostForm.Get("replication_factor"))
		//t.Config = r.PostForm.Get("config")
	}

	fmt.Println(t)
	return t, err
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

func createTopic(w http.ResponseWriter, t topicData) {
	err := zookeeper.CreateTopic(t.Name, t.Partitions, t.ReplicationFactor, t.Config)
	if err != nil {
		internalError(w, err)
		return
	}
	topic(w, t.Name)
}

func updateTopic(w http.ResponseWriter, name string, t topicData) {
	err := zookeeper.UpdateTopic(name, t.Partitions, t.ReplicationFactor, t.Config)
	if err != nil {
		internalError(w, err)
		return
	}
	topic(w, name)
}
