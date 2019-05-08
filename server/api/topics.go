package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/db"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	bolt "go.etcd.io/bbolt"
	"goji.io/pat"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topicNames, err := zookeeper.Topics(p)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	metricRequests := make([]store.MetricRequest, len(config.BrokerUrls)*2)
	i := 0
	for id, _ := range config.BrokerUrls {
		//metricRequests[i] = store.PartitionLogStartOffset(id, "*")
		//metricRequests[i+1] = store.PartitionLogEndOffset(id, "*")
		//metricRequests[i+2] = store.PartitionSize(id, "*")
		metricRequests[i] = store.MetricRequest{id, store.BeanAllTopicsBytesInPerSec, "OneMinuteRate"}
		metricRequests[i+2] = store.MetricRequest{id, store.BeanAllTopicsLogSize, "Value"}
		//metricRequests[i] = store.TopicBytesOutPerSec(id)
		i = i + 1
	}
	req := store.TopicRequest{
		TopicNames: topicNames,
		Config:     false,
		Metrics:    metricRequests,
	}
	topics, err := store.FetchTopics(ctx, req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Topic.Name < topics[j].Topic.Name
	})
	ts := make([]interface{}, len(topics))
	for i, t := range topics {
		if t.Error != nil {
			ts[i] = t.Error
		} else {
			ts[i] = t.Topic
		}
	}
	fmt.Println(ts)
	writeAsJson(w, ts)
}

func Topic(w http.ResponseWriter, r *http.Request) {
	topicName := pat.Param(r, "name")
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	zkPath := fmt.Sprintf("/brokers/topics/%s", topicName)
	if !zookeeper.Exists(zkPath) {
		http.NotFound(w, r)
		return
	}
	metricRequests := make([]store.MetricRequest, len(config.BrokerUrls)*3)
	i := 0
	//metricNames := []string{"MessagesInPerSec", "BytesRejectedPerSec", "BytesOutPerSec", "BytesInPerSec"}
	for id, _ := range config.BrokerUrls {
		metricRequests[i] = store.MetricRequest{id, store.BeanTopicBytesInPerSec(topicName), "OneMinuteRate"}
		metricRequests[i+1] = store.MetricRequest{id, store.BeanTopicBytesOutPerSec(topicName), "OneMinuteRate"}
		metricRequests[i+2] = store.MetricRequest{id, store.BeanTopicLogSize(topicName), "Value"}
		i = i + 3
	}
	req := store.TopicRequest{
		TopicNames: []string{topicName},
		Config:     true,
		Metrics:    metricRequests,
	}
	topics, err := store.FetchTopics(ctx, req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	writeAsJson(w, topics[0].Topic)
}

func TopicThroughput(w http.ResponseWriter, r *http.Request) {
	topicName := pat.Param(r, "name")
	back := time.Duration(2)
	if keys, ok := r.URL.Query()["from"]; ok {
		from, err := strconv.Atoi(keys[0])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Param 'from' must a an integer"))
			return
		}
		back = time.Duration(from)
	}
	from := time.Now().Add(time.Hour * back * -1)
	metrics := map[string]string{
		"messages_MessagesInPerSec": "Messages in",
		"bytes_BytesInPerSec":       "Bytes in",
		"bytes_BytesOutPerSec":      "Bytes out",
		"bytes_BytesRejectedPerSec": "Bytes rejected",
	}
	db.View(func(tx *bolt.Tx) error {
		var res []*db.Serie
		for k, name := range metrics {
			key_split := strings.Split(k, "_")
			s := &db.Serie{
				Name: name,
				Type: key_split[0],
				Data: make([]db.DataPoint, 0),
			}
			for brokerId, _ := range config.BrokerUrls {
				path := fmt.Sprintf("topic_metrics/%s/%s/%d", topicName, key_split[1], brokerId)
				s.Add(db.TimeSerie(tx, path, from))
			}
			if len(s.Data) > 0 {
				res = append(res, s)
			}
		}
		if len(res) == 0 {
			w.WriteHeader(http.StatusNoContent)
		}
		writeAsJson(w, res)
		return nil
	})
}

func CreateTopic(w http.ResponseWriter, r *http.Request) {
	var topic TopicModel
	err := parseRequestBody(r, &topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if errors := topic.Validate(); len(errors) > 0 {
		msg := fmt.Sprintf("Could not validate topic config:\n* %s",
			strings.Join(errors, "\n* "))
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	err = zookeeper.CreateTopic(topic.Name, topic.PartitionCount, topic.ReplicationFactor, topic.Config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateTopic: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("[INFO] action=create-topic topic=%s\n", topic.Name)
	w.WriteHeader(http.StatusCreated)
}

func UpdateTopic(w http.ResponseWriter, r *http.Request) {
	var topic TopicModel
	err := parseRequestBody(r, &topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if errors := topic.Validate(); len(errors) > 0 {
		msg := fmt.Sprintf("Could not validate topic config:\n* %s",
			strings.Join(errors, "\n* "))
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	err = zookeeper.UpdateTopic(topic.Name, topic.PartitionCount, topic.ReplicationFactor, topic.Config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.UpdateTopic: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("[INFO] action=update-topic topic=%s\n", topic.Name)
}

func DeleteTopic(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	if err := zookeeper.DeleteTopic(name); err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Printf("[INFO] action=delete-topic topic=%s\n", name)
}
