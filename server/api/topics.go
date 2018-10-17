package api

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/db"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	bolt "go.etcd.io/bbolt"
	"goji.io/pat"
)

func zkTopic(name string, partMetrics map[string]interface{}) map[string]interface{} {
	t, err := zookeeper.Topic(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR], api.zkTopic: Cannot get topic from Zookeeper: %s", err)
		return nil
	}
	partitionCount := len(t.Partitions)
	partitions := make([]map[string]interface{}, partitionCount)
	var msgCount float64 = 0
	var topicSize float64 = 0
	for num, replicas := range t.Partitions {
		p, _ := zookeeper.Partition(t.Name, num)
		n, _ := strconv.Atoi(num)
		partitions[n] = map[string]interface{}{
			"number":   num,
			"leader":   p.Leader,
			"replicas": replicas,
			"isr":      p.Isr,
		}
		if partMetrics != nil {
			if pm, ok := partMetrics[num].(map[string]interface{}); ok {
				for k, v := range pm {
					partitions[n][k] = v
				}
				topicSize += pm["log_size"].(float64)
				msgCount += pm["log_end"].(float64) - pm["log_start"].(float64)
			}
		}
	}
	return map[string]interface{}{
		"config":             t.Config,
		"partition_count":    len(t.Partitions),
		"replication_factor": len(t.Partitions["0"]),
		"partitions":         partitions,
		"message_count":      msgCount,
		"size":               topicSize,
	}
}

func topicMetricSum(tx *bolt.Tx, id string) map[string]interface{} {
	res := map[string]interface{}{
		"messages_in":    0,
		"bytes_in":       0,
		"bytes_out":      0,
		"bytes_rejected": 0,
	}
	for k, _ := range res {
		if b := db.BucketByPath(tx, fmt.Sprintf("brokers/%s/%s", id, k)); b != nil {
			res[k] = db.Last(b)
		}
	}
	return res
}

func Topics(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	err := db.View(func(tx *bolt.Tx) error {
		topics, _ := zookeeper.Topics(p)
		res := make([]map[string]interface{}, len(topics))
		for i, topicName := range topics {
			topic, _ := zookeeper.Topic(topicName)
			res[i] = map[string]interface{}{
				"name":               topicName,
				"partition_count":    len(topic.Partitions),
				"replication_factor": len(topic.Partitions["0"]),
			}
			b := db.BucketByPath(tx, "topics/"+topicName)
			if b != nil {
				for k, v := range db.Values(b) {
					res[i][k] = v
				}
			}
		}
		writeAsJson(w, res)
		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func Topic(w http.ResponseWriter, r *http.Request) {
	id := pat.Param(r, "name")
	err := db.View(func(tx *bolt.Tx) error {
		b := db.BucketByPath(tx, fmt.Sprintf("topics/%s/partitions", id))
		var partitions map[string]interface{}
		if b != nil {
			partitions = db.Recur(b, 2)
		}
		topic := zkTopic(id, partitions)
		if topic == nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "Topic %s not found", id)
			return nil
		}
		for k, v := range topicMetricSum(tx, id) {
			topic[k] = v
		}
		writeAsJson(w, topic)
		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func TopicThroughput(w http.ResponseWriter, r *http.Request) {
	id := pat.Param(r, "name")
	from := time.Now().Add(time.Minute * 30 * -1)
	metrics := map[string]string{
		"messages_in":    "Messages in",
		"bytes_in":       "Bytes in",
		"bytes_out":      "Bytes out",
		"bytes_rejected": "Bytes rejected",
	}
	db.View(func(tx *bolt.Tx) error {
		var res []*db.Serie
		for k, name := range metrics {
			key_split := strings.Split(k, "_")
			path := fmt.Sprintf("topics/%s/%s", id, k)
			s := &db.Serie{
				Name: name,
				Type: key_split[0],
				Data: db.TimeSerie(tx, path, from),
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
