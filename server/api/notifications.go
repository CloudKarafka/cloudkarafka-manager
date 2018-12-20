package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/84codes/cloudkarafka-mgmt/config"
	n "github.com/84codes/cloudkarafka-mgmt/notifications"
	bolt "go.etcd.io/bbolt"
)

type notification struct {
	Type    string `json:"type"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type notificationFn func(tx *bolt.Tx) ([]notification, bool)

var (
	fns = []notificationFn{
		checkUnevenPartitions,
		checkUnevenLeaders,
		checkURP,
	}
)

func checkUneven(grouped map[string]float64, msg string) ([]notification, bool) {
	var (
		n   []notification
		any = false
	)
	var sum float64 = 0
	for _, count := range grouped {
		sum += count
	}
	for id, count := range grouped {
		if sum == 0 {
			break
		}
		q := float64(len(grouped)) * count / float64(sum)
		var (
			level string
		)
		if q < 0.2 {
			level = "danger"
		} else if q < 0.5 {
			level = "warning"
		} else if q < 0.8 {
			level = "info"
		}
		if level != "" {
			n = append(n, notification{
				Type:    "Uneven spread",
				Level:   level,
				Message: fmt.Sprintf(msg, id, int(q*100), sum),
			})
			any = true
		}
	}
	return n, any
}

func checkUnevenPartitions(tx *bolt.Tx) ([]notification, bool) {
	msg := "Your cluster has a uneven partition spread. Broker %s has %v%% of the total number (%v) of partitions. This will have negative impact on the throughput."
	group := make(map[string]float64)
	/*
		ids := brokerIds(tx)
		for _, id := range ids {
			b := brokerInfo(tx, id)
			group[id] = b["partition_count"].(float64)
		}
	*/
	return checkUneven(group, msg)
}

func checkUnevenLeaders(tx *bolt.Tx) ([]notification, bool) {
	msg := "Your cluster has an uneven leader spread. Broker %s is leader of %v%% of the total number (%v) of partitions. This will have negative impact o>n the throughput."
	group := make(map[string]float64)
	/*
		ids := brokerIds(tx)
		for _, id := range ids {
			b := brokerInfo(tx, id)
			group[id] = b["leader_count"].(float64)
		}
	*/
	return checkUneven(group, msg)
}
func checkURP(tx *bolt.Tx) ([]notification, bool) {
	//msg := "Broker %s has %.0f under replicated partitions"
	var n []notification
	any := false
	/*
		ids := brokerIds(tx)
		for _, id := range ids {
			b := brokerInfo(tx, id)
			v := b["under_replicated_partitions"].(float64)
			if v > 0 {
				n = append(n, notification{
					Type:    "Under replicated partitions",
					Level:   "danger",
					Message: fmt.Sprintf(msg, id, v),
				})
				any = true
			}
		}
	*/
	return n, any
}

func Notifications(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	writeAsJson(w, n.List(ctx))
}
