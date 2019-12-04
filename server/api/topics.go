package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/server/validators"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	fmt.Println("TOPICS!!!")
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListTopics() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

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
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ReadTopic(topicName) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
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
	var (
		err               error
		data              map[string]interface{}
		ok                bool
		name              string
		replicationFactor float64
		partitions        float64
		config            map[string]string
	)
	user := r.Context().Value("user").(mw.SessionUser)

	err = parseRequestBody(r, &data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if name, ok = data["name"].(string); !ok {
		http.Error(w, "Name must be a string", http.StatusBadRequest)
		return
	}
	if errs := validators.ValidateTopicName(name); len(errs) > 0 {
		http.Error(w, strings.Join(errs, "\n"), http.StatusBadRequest)
		return
	}
	if !user.Permissions.CreateTopic(name) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if replicationFactor, ok = data["replication_factor"].(float64); !ok {
		http.Error(w, "replication_factor must be an integer", http.StatusBadRequest)
		return
	}
	if replicationFactor <= 0 {
		http.Error(w, "Replication factor cannot be zero", http.StatusBadRequest)
		return
	}
	if partitions, ok = data["partitions"].(float64); !ok {
		http.Error(w, "partitions must be an integer", http.StatusBadRequest)
		return
	}
	if partitions <= 0 {
		http.Error(w, "Topic must have at least one partition", http.StatusBadRequest)
		return
	}
	if data["config"] != nil {
		if config, ok = data["config"].(map[string]string); !ok {
			http.Error(w, "config must be a hashmap of string=>string", http.StatusBadRequest)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	err = store.CreateTopic(ctx, name, int(partitions), int(replicationFactor), config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func UpdateTopic(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		data map[string]interface{}
		ok   bool
		name = pat.Param(r, "name")
	)
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ReadTopic(name) || !user.Permissions.UpdateTopic(name) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !zookeeper.Exists(fmt.Sprintf("/brokers/topics/%s", name)) {
		http.NotFound(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	err = parseRequestBody(r, &data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if data["partitions"] != nil {
		topic, err := store.FetchTopic(ctx, name, false, nil)
		if err != nil {
			log.Error("api.topic", log.ErrorEntry{err})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var partitions_f float64
		if partitions_f, ok = data["partitions"].(float64); !ok {
			http.Error(w, "partitions must be an integer", http.StatusBadRequest)
			return
		}
		partitions := int(partitions_f)
		if partitions <= len(topic.Partitions) {
			msg := fmt.Sprintf("You can only add partitions to topic, topic has %d partitions", len(topic.Partitions))
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
		if err = store.AddParitions(ctx, name, partitions); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if data["config"] != nil {
		var config map[string]interface{}
		if config, ok = data["config"].(map[string]interface{}); !ok {
			http.Error(w, "config must be a hashmap of string=>string", http.StatusBadRequest)
			return
		}
		if len(config) > 0 {
			if err = store.UpdateTopicConfig(ctx, name, config); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

func DeleteTopic(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.DeleteTopic(name) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := store.DeleteTopic(ctx, name); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}
