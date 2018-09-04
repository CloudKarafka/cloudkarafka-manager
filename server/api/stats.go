package api

import (
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"
	"net/http"
)

type AStats struct {
	BrokerId string                 `json:"id"`
	Stats    map[string]interface{} `json:"stats"`
}

func AdminStats(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	var data []AStats
	for _, id := range store.Brokers() {
		b := store.Broker(id)
		data = append(data, AStats{
			BrokerId: id,
			Stats: map[string]interface{}{
				"version": b.Version,
				"urp":     b.UnderReplicatedPartitions,
			},
		})
	}
	WriteJson(w, data)
}
