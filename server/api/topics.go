package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	topicNames, err := zookeeper.Topics(user.Permissions)
	if err != nil {
		log.Error("api.list_topics", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topics, err := store.FetchTopics(ctx, topicNames, false, nil)
	if err != nil {
		log.Error("api.list_topics", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ts := make([]interface{}, len(topics))
	i := 0
	for _, t := range topics {
		if t.Error != nil {
			log.Error("api.list_topics", log.ErrorEntry{err})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else {
			ts[i] = t.Topic
			i += 1
		}
	}
	writeAsJson(w, ts[:i])
}

func Topic(w http.ResponseWriter, r *http.Request) {
	topicName := pat.Param(r, "name")
	if !zookeeper.Exists(fmt.Sprintf("/brokers/topics/%s", topicName)) {
		http.NotFound(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topic, err := store.FetchTopic(ctx, topicName, true, nil)
	if err != nil {
		log.Error("api.topic", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeAsJson(w, topic)
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
