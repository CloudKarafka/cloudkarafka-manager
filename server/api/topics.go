package api

import (
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var (
	replicationFactorRequired = errors.New("ERROR: must suply replication_factor and it must be numeric.")
	partitionsRequired        = errors.New("ERROR: must suply partitions and it must be numeric")
)

type topicVM struct {
	zookeeper.T
	PartitionCount    int `json:"partition_count,1"`
	ReplicationFactor int `json:"replication_factor,1"`
	BrokerSpread      int `json:"broker_spread"`
}

type partitionVM struct {
	zookeeper.P
	jmx.OffsetMetric
}

func Topics(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	switch r.Method {
	case "GET":
		topics(w, p)
	case "POST":
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err.Error())
		} else {
			createTopic(w, t)
		}
	default:
		http.NotFound(w, r)
	}
}

func Topic(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		if !p.TopicRead(vars["topic"]) {
			http.NotFound(w, r)
			return
		}
		getTopic(w, vars["topic"])
	case "PUT":
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err.Error())
		} else {
			updateTopic(w, vars["topic"], t)
		}
	case "DELETE":
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		deleteTopic(w, vars["topic"])
	default:
		http.NotFound(w, r)
	}
}

func ReassigningTopic(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	if !p.ClusterWrite() {
		http.NotFound(w, r)
	}
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		rp, err := zookeeper.ReassigningPartitions(vars["topic"])
		if err != nil {
			internalError(w, err.Error())
			return
		}
		writeJson(w, rp)
	default:
		http.NotFound(w, r)
	}

}

func TopicMetrics(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	bm, err := jmx.TopicMetrics(vars["topic"])
	if err != nil {
		internalError(w, err)
	} else {
		writeJson(w, bm)
	}
}

func Config(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	if !p.TopicRead(vars["topic"]) {
		http.NotFound(w, r)
		return
	}
	cfg, err := zookeeper.Config(vars["topic"])
	if err != nil {
		internalError(w, err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(cfg))
	}
}

func Partition(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	part, err := zookeeper.Partition(vars["topic"], vars["partition"])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	om, err := fetchOffsetMetric(vars["topic"], vars["partition"], r)
	if err != nil {
		fmt.Println(err)
	}
	partition := partitionVM{P: part, OffsetMetric: om}
	writeJson(w, partition)
}

func PartitionMetrics(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	fmt.Println(vars["topic"], vars["partition"])
	om, err := jmx.LogOffsetMetric(vars["topic"], vars["partition"])
	if err != nil {
		fmt.Println("[ERROR]", err)
		internalError(w, err)
	} else {
		writeJson(w, om)
	}
}

func decodeTopic(r *http.Request) (topicVM, error) {
	var (
		t   topicVM
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
		t.PartitionCount, err = strconv.Atoi(r.PostForm.Get("partition_count"))
		t.ReplicationFactor, err = strconv.Atoi(r.PostForm.Get("replication_factor"))
		if r.PostForm.Get("config") != "" {
			rows := strings.Split(r.PostForm.Get("config"), "\n")
			cfg := make(map[string]interface{})
			for _, r := range rows {
				cols := strings.Split(r, "=")
				key := strings.Trim(cols[0], " \n\r")
				val := strings.Trim(cols[1], " \n\r")
				cfg[key] = val
			}
			t.Config = cfg
		}
	}
	return t, err
}

func getTopic(w http.ResponseWriter, name string) {
	top, err := zookeeper.Topic(name)
	if err != nil {
		http.NotFound(w, nil)
		return
	}

	var (
		t topicVM
	)
	t = topicVM{T: top}
	t.PartitionCount = len(t.Partitions)
	t.ReplicationFactor = len(t.Partitions["0"])
	writeJson(w, t)
}

func deleteTopic(w http.ResponseWriter, topic string) {
	err := zookeeper.DeleteTopic(topic)
	if err != nil {
		internalError(w, err.Error())
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func topics(w http.ResponseWriter, p zookeeper.Permissions) {
	topics, err := zookeeper.Topics(p)
	if err != nil {
		internalError(w, err.Error())
	} else {
		writeJson(w, topics)
	}
}

func createTopic(w http.ResponseWriter, t topicVM) {
	err := zookeeper.CreateTopic(t.Name, t.PartitionCount, t.ReplicationFactor, t.Config)
	if err != nil {
		internalError(w, err.Error())
		return
	}
	getTopic(w, t.Name)
}

func updateTopic(w http.ResponseWriter, name string, t topicVM) {
	err := zookeeper.UpdateTopic(name, t.PartitionCount, t.ReplicationFactor, t.Config)
	if err != nil {
		fmt.Println(err)
		internalError(w, err.Error())
		return
	}
	getTopic(w, name)
}
