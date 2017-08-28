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

type topic struct {
	Name              string                 `json:"name,omitempty"`
	PartitionCount    int                    `json:"partition_count,1"`
	ReplicationFactor int                    `json:"replication_factor,1"`
	Config            map[string]interface{} `json:"config"`
	Metrics           map[string]interface{} `json:"metrics"`
	Partitions        map[string][]int       `json:"partitions"`
}

type partition struct {
	Number         string `json:"number"`
	Topic          string `json:"topic"`
	Leader         int    `json:"leader"`
	Isr            []int  `json:"isr"`
	LogEndOffset   int    `json:"log_end_offset"`
	LogStartOffset int    `json:"log_start_offset"`
}

func Topics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		topics(w)
	case "POST":
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err.Error())
		} else {
			createTopic(w, t)
		}
	}
}

func Topic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		getTopic(w, vars["topic"])
	case "PUT":
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err.Error())
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
		internalError(w, err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(cfg))
	}
}

func Partition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	p := partition{Topic: vars["topic"], Number: vars["partition"]}
	part, err := zookeeper.Partition(p.Topic, p.Number)
	if err != nil {
		internalError(w, p)
		return
	}
	json.Unmarshal(part, &p)

	p.LogStartOffset, err = jmx.LogOffset("LogStartOffset", vars["topic"], vars["partition"])
	if err != nil {
		internalError(w, p)
		return
	}
	p.LogEndOffset, err = jmx.LogOffset("LogEndOffset", vars["topic"], vars["partition"])
	if err != nil {
		internalError(w, p)
		return
	}

	if err != nil {
		internalError(w, p)
		return
	} else {
		writeJson(w, p)
	}
}

func decodeTopic(r *http.Request) (topic, error) {
	var (
		t   topic
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
	t := topic{Name: name}
	top, err := zookeeper.Topic(name)
	if len(top) == 0 {
		http.NotFound(w, nil)
		return
	}
	err = json.Unmarshal(top, &t)
	if err != nil {
		fmt.Println(err)
		internalError(w, t)
		return
	}
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

func topics(w http.ResponseWriter) {
	topics, err := zookeeper.Topics()
	if err != nil {
		internalError(w, err.Error())
	} else {
		writeJson(w, topics)
	}
}

func createTopic(w http.ResponseWriter, t topic) {
	err := zookeeper.CreateTopic(t.Name, t.PartitionCount, t.ReplicationFactor, t.Config)
	if err != nil {
		internalError(w, err.Error())
		return
	}
	getTopic(w, t.Name)
}

func updateTopic(w http.ResponseWriter, name string, t topic) {
	err := zookeeper.UpdateTopic(name, t.PartitionCount, t.ReplicationFactor, t.Config)
	if err != nil {
		fmt.Println(err)
		internalError(w, err.Error())
		return
	}
	getTopic(w, name)
}
