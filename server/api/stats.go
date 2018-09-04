package api

import (
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"
	"net/http"
)

type BrokerStatsData struct {
	BrokerId string                 `json:"id"`
	Stats    map[string]interface{} `json:"stats"`
}

func BrokerStats(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	brokers := store.Brokers()
	res := make([]BrokerStatsData, len(brokers))
	i := 0
	for _, id := range brokers {
		b := store.Broker(id)
		res[i] = BrokerStatsData{
			BrokerId: id,
			Stats: map[string]interface{}{
				"under_replicated_partitions": b.UnderReplicatedPartitions,
			},
		}
	}
	WriteJson(w, res)
}
