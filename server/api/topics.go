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
	m "github.com/cloudkarafka/cloudkarafka-manager/metrics"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	bolt "go.etcd.io/bbolt"
	"goji.io/pat"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topics, err := m.FetchTopicList(ctx, p)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	metricNames := []string{"BytesOutPerSec", "BytesInPerSec"}
	metrics, err := m.FetchTopicMetrics(ctx, "*", metricNames)
	res := make([]map[string]interface{}, len(topics))
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	for i, topic := range topics {
		res[i] = map[string]interface{}{
			"topic":   topic,
			"metrics": metrics[topic.Name],
		}
	}
	writeAsJson(w, res)
}

func Topic(w http.ResponseWriter, r *http.Request) {
	topicName := pat.Param(r, "name")
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topic, err := m.FetchTopic(ctx, topicName)
	if err != nil {
		if err == zookeeper.PathDoesNotExistsErr {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	metricNames := []string{"MessagesInPerSec", "BytesRejectedPerSec", "BytesOutPerSec", "BytesInPerSec"}
	metrics, err := m.FetchTopicMetrics(ctx, topicName, metricNames)
	config, err := m.FetchTopicConfig(ctx, topicName)

	res := map[string]interface{}{
		"details": topic,
		"config":  config,
		"metrics": metrics[topic.Name],
	}
	writeAsJson(w, res)
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
