package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/server/validators"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func Topics(w http.ResponseWriter, r *http.Request) {
	var (
		user = r.Context().Value("user").(mw.SessionUser)
	)
	if !user.Permissions.ListTopics() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	topics := store.Topics()
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	ps, p, err := pageInfo(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeAsJson(w, Page(ps, p, topics))
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

	topic, ok := store.Topic(topicName)
	if !ok {
		http.NotFound(w, r)
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

		config = make(map[string]string)
	)
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.CreateTopic(name) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	err = parseRequestBody(r, &data)
	if err != nil {
		jsonError(w, err.Error())
		return
	}
	if name, ok = data["name"].(string); !ok {
		jsonError(w, "Name must be a string")
		return
	}
	if errs := validators.ValidateTopicName(name); len(errs) > 0 {
		jsonError(w, strings.Join(errs, "\n"))
		return
	}
	if replicationFactor, ok = data["replication_factor"].(float64); !ok {
		jsonError(w, "replication_factor must be an integer")
		return
	}
	if replicationFactor <= 0 {
		jsonError(w, "Replication factor cannot be zero")
		return
	}
	if partitions, ok = data["partitions"].(float64); !ok {
		jsonError(w, "partitions must be an integer")
		return
	}
	if partitions <= 0 {
		jsonError(w, "Topic must have at least one partition")
		return
	}
	fmt.Println(data["config"].(map[string]interface{}))
	if data["config"] != nil {
		cfg, ok := data["config"].(map[string]interface{})
		if !ok {
			jsonError(w, "config must be a hashmap of string=>string")
			return
		}
		for k, v := range cfg {
			if config[k], ok = v.(string); !ok {
				continue
			}
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	err = store.CreateTopic(ctx, name, int(partitions), int(replicationFactor), config)
	if err != nil {
		jsonError(w, err.Error())
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
		jsonError(w, err.Error())
		return
	}
	if data["partitions"] != nil {
		topic, ok := store.Topic(name)
		if !ok {
			http.NotFound(w, r)
			return
		}
		var partitions_f float64
		if partitions_f, ok = data["partitions"].(float64); !ok {
			jsonError(w, "partitions must be an integer")
			return
		}
		partitions := int(partitions_f)
		if partitions <= len(topic.Partitions) {
			msg := fmt.Sprintf("You can only add partitions to topic, topic has %d partitions", len(topic.Partitions))
			jsonError(w, msg)
			return
		}
		if err = store.AddParitions(ctx, name, partitions); err != nil {
			jsonError(w, err.Error())
			return
		}
	}
	if data["config"] != nil {
		var config map[string]interface{}
		if config, ok = data["config"].(map[string]interface{}); !ok {
			jsonError(w, "config must be a hashmap of string=>string")
			return
		}
		if len(config) > 0 {
			if err = store.UpdateTopicConfig(ctx, name, config); err != nil {
				jsonError(w, err.Error())
				return
			}
		}
	}
	store.UpdateTopic(name)
	w.WriteHeader(http.StatusOK)
}

func DeleteTopic(w http.ResponseWriter, r *http.Request) {
	var (
		name = pat.Param(r, "name")
		user = r.Context().Value("user").(mw.SessionUser)
	)
	if !user.Permissions.DeleteTopic(name) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	if err := store.DeleteTopic(ctx, name); err != nil {
		jsonError(w, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func Partitions(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	var (
		name = pat.Param(r, "name")
		user = r.Context().Value("user").(mw.SessionUser)
	)
	if !user.Permissions.ReadTopic(name) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	topic, ok := store.Topic(name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	ps, p, err := pageInfo(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeAsJson(w, Page(ps, p, topic.Partitions))
}
