package api

import (
	"fmt"
	"net/http"
	"os"

	"github.com/84codes/cloudkarafka-mgmt/db"
	bolt "go.etcd.io/bbolt"
	"goji.io/pat"
)

func Consumers(w http.ResponseWriter, r *http.Request) {
	var res []map[string]interface{}
	err := db.View(func(tx *bolt.Tx) error {
		groupBucket := tx.Bucket([]byte("groups"))
		if groupBucket != nil {
			groupBucket.ForEach(func(k, v []byte) error {
				topicBucket := groupBucket.Bucket(k)
				topics := []string{}
				topicBucket.ForEach(func(k, _ []byte) error {
					topics = append(topics, string(k))
					return nil
				})
				clients := make(map[string]bool)
				lag := 0
				db.Dig(topicBucket, 2, func(d map[string]interface{}) {
					clients[d["clientid"].(string)] = true
					lag += int(d["log_end_offset"].(float64) - d["current_offset"].(float64))
				})
				topic := map[string]interface{}{
					"name":    string(k),
					"clients": len(clients),
					"lag":     lag,
					"topics":  topics,
				}
				res = append(res, topic)
				return nil
			})
		}
		//res = db.Recur(b, 3)
		writeAsJson(w, res)
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.Consumers: %s\n", err)
		http.Error(w, "Database error, please contact support", http.StatusInternalServerError)
		return
	}
}

func Consumer(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	var res map[string]interface{}
	err := db.View(func(tx *bolt.Tx) error {
		path := fmt.Sprintf("groups/%s", name)
		b := db.BucketByPath(tx, path)
		if b == nil {
			return nil
		}
		res = db.Recur(b, 2)
		writeAsJson(w, res)
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.Consumer: %s\n", err)
		http.Error(w, "Database error, please contact support", http.StatusInternalServerError)
		return
	}
}
