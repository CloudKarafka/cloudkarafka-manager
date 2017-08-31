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
)

var (
	replicationFactorRequired = errors.New("ERROR: must suply replication_factor and it must be numeric.")
	partitionsRequired        = errors.New("ERROR: must suply partitions and it must be numeric")
)

type topicVM struct {
	zookeeper.T
	PartitionCount    int                    `json:"partition_count,1"`
	ReplicationFactor int                    `json:"replication_factor,1"`
	Metrics           map[string]interface{} `json:"metrics"`
}

type partitionVM struct {
	zookeeper.P
	LogEndOffset   int `json:"log_end_offset"`
	LogStartOffset int `json:"log_start_offset"`
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
		if !(p.ClusterRead() || p.TopicRead(vars["topic"])) {
			http.NotFound(w, r)
			return
		}
		getTopic(w, vars["topic"])
	case "PUT":
		if !(p.ClusterWrite() || p.TopicWrite(vars["topic"])) {
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
		if !(p.ClusterWrite() || p.TopicWrite(vars["topic"])) {
			http.NotFound(w, r)
			return
		}
		deleteTopic(w, vars["topic"])
	default:
		http.NotFound(w, r)
	}
}

func Config(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	vars := mux.Vars(r)
	if !(p.ClusterRead() || p.TopicRead(vars["topic"])) {
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
	partition := partitionVM{P: part}

	partition.LogStartOffset, err = jmx.LogOffset("LogStartOffset", vars["topic"], vars["partition"])
	if err != nil {
		internalError(w, partition)
		return
	}
	partition.LogEndOffset, err = jmx.LogOffset("LogEndOffset", vars["topic"], vars["partition"])
	if err != nil {
		internalError(w, partition)
		return
	}

	if err != nil {
		internalError(w, partition)
		return
	} else {
		writeJson(w, partition)
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
		//t.Config = r.PostForm.Get("config")
	}
	return t, err
}

func getTopic(w http.ResponseWriter, name string) {
	top, err := zookeeper.Topic(name)
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	t := topicVM{T: top}
	t.PartitionCount = len(t.Partitions)
	t.ReplicationFactor = len(t.Partitions["0"])

	metrics := make(map[string]interface{})
	if d, err := jmx.BrokerTopicMetric("BytesOutPerSec", t.Name); err != nil {
		fmt.Println(err)
	} else {
		metrics["bytes_out_per_sec"] = d
	}
	if d, err := jmx.BrokerTopicMetric("BytesInPerSec", t.Name); err != nil {
		fmt.Println(err)
	} else {
		metrics["bytes_in_per_sec"] = d
	}
	if d, err := jmx.BrokerTopicMetric("MessagesInPerSec", t.Name); err != nil {
		fmt.Println(err)
	} else {
		metrics["messages_in_per_sec"] = d
	}
	t.Metrics = metrics
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
