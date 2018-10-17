package api

import (
	"fmt"
	"net/http"
	"os"

	"github.com/84codes/cloudkarafka-mgmt/db"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	bolt "go.etcd.io/bbolt"
)

func topicOverview(p zookeeper.Permissions, tx *bolt.Tx, res map[string]int64) map[string]int64 {
	res["topic_size"] = 0
	res["topic_msg_count"] = 0
	res["topic_count"] = 0
	topics, err := zookeeper.Topics(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.topicOverview: %s", err)
		return res
	}
	for _, topic := range topics {
		tb := db.BucketByPath(tx, fmt.Sprintf("topics/%s/partitions", topic))
		if tb != nil {
			tb.ForEach(func(k, v []byte) error {
				if v == nil {
					partBucket := tb.Bucket(k)
					size := db.GetFloat(partBucket, "log_size")
					start := db.GetFloat(partBucket, "log_start")
					end := db.GetFloat(partBucket, "log_end")
					res["topic_size"] += int64(size)
					res["topic_msg_count"] += int64(end - start)
				}
				return nil
			})
			res["topic_count"] += 1
		}
	}
	return res
}
func brokerOverview(tx *bolt.Tx, res map[string]int64) map[string]int64 {
	res["broker_count"] = 0
	b := tx.Bucket([]byte("brokers"))
	if b == nil {
		return res
	}
	b.ForEach(func(k, v []byte) error {
		res["broker_count"] += 1
		return nil
	})
	return res
}

// TODO Show only consumer that consumes from topics that user has permissions for?
func consumerOverview(tx *bolt.Tx, res map[string]int64) map[string]int64 {
	res["consumer_count"] = 0
	b := tx.Bucket([]byte("groups"))
	if b == nil {
		return res
	}
	b.ForEach(func(k, v []byte) error {
		res["consumer_count"] += 1
		return nil
	})

	return res
}

func userOverview(p zookeeper.Permissions, tx *bolt.Tx, res map[string]int64) map[string]int64 {
	res["user_count"] = 0
	users, err := zookeeper.Users(p)
	if err != nil {
		return res
	}
	res["user_count"] = int64(len(users))
	return res
}

func Overview(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	err := db.View(func(tx *bolt.Tx) error {
		res := make(map[string]int64)
		res = topicOverview(p, tx, res)
		res = brokerOverview(tx, res)
		res = consumerOverview(tx, res)
		res = userOverview(p, tx, res)
		writeAsJson(w, res)
		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

}
