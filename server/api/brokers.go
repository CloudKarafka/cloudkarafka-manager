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
	humanize "github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"goji.io/pat"
)

var metrics = map[string]string{
	"messages_in":    "Messages in",
	"bytes_in":       "Bytes in",
	"bytes_out":      "Bytes out",
	"bytes_rejected": "Bytes rejected",
}

func brokerIds(tx *bolt.Tx) []string {
	res := make([]string, 0)
	b := db.BucketByPath(tx, "brokers")
	if b == nil {
		return res
	}
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		res = append(res, string(k))
	}
	return res
}

func brokerMetrics(tx *bolt.Tx, id string) map[string]interface{} {
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

func zkBroker(id string) (map[string]interface{}, error) {
	b, err := zookeeper.Broker(id)
	if err != nil {
		return nil, err
	}
	ts, err := strconv.ParseInt(b.Timestamp, 10, 64)
	data := map[string]interface{}{
		"endpoints": strings.Join(b.Endpoints, ","),
		"uptime":    strings.TrimSpace(humanize.RelTime(time.Now(), time.Unix(ts/1000, 0), "", "")),
	}
	return data, nil
}

func brokerInfo(tx *bolt.Tx, id string) map[string]interface{} {
	path := "brokers/" + id
	b := db.BucketByPath(tx, path)
	if b == nil {
		return nil
	}
	res := db.Values(b)
	res["broker_id"] = id
	zkData, err := zkBroker(id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] No broker info in ZK for broker %s: %s", id, err)
		res["online"] = false
	} else {
		res["online"] = true
		for k, v := range zkData {
			res[k] = v
		}
	}
	for k, v := range brokerMetrics(tx, id) {
		res[k] = v
	}
	return res
}

func Brokers(w http.ResponseWriter, r *http.Request) {
	db.View(func(tx *bolt.Tx) error {
		res := make([]map[string]interface{}, 0)
		ids := brokerIds(tx)
		for _, id := range ids {
			if b := brokerInfo(tx, id); b != nil {
				res = append(res, b)
			}
		}
		writeAsJson(w, res)
		return nil
	})
}

func Broker(w http.ResponseWriter, r *http.Request) {
	id := pat.Param(r, "id")
	db.View(func(tx *bolt.Tx) error {
		res := brokerInfo(tx, id)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
		}
		writeAsJson(w, res)
		return nil
	})
}

func BrokersThroughput(w http.ResponseWriter, r *http.Request) {
	from := time.Now().Add(time.Minute * 30 * -1)
	db.View(func(tx *bolt.Tx) error {
		brokerIds := brokerIds(tx)
		var res []*db.Serie
		for k, name := range metrics {
			key_split := strings.Split(k, "_")
			s := &db.Serie{Name: name, Type: key_split[0]}
			for _, id := range brokerIds {
				path := fmt.Sprintf("brokers/%s/%s", id, k)
				d := db.TimeSerie(tx, path, from)
				s.Add(d)
			}
			res = append(res, s)
		}
		writeAsJson(w, res)
		return nil
	})
}
func BrokerThroughput(w http.ResponseWriter, r *http.Request) {
	id := pat.Param(r, "id")
	from := time.Now().Add(time.Minute * 30 * -1)
	db.View(func(tx *bolt.Tx) error {
		var res []*db.Serie
		for k, name := range metrics {
			key_split := strings.Split(k, "_")
			path := fmt.Sprintf("brokers/%s/%s", id, k)
			res = append(res, &db.Serie{
				Name: name,
				Type: key_split[0],
				Data: db.TimeSerie(tx, path, from),
			})
		}
		writeAsJson(w, res)
		return nil
	})
}
