package api

import (
	"encoding/json"
	"net/http"

	"github.com/84codes/cloudkarafka-mgmt/metrics"
)

func MetricsBatch(w http.ResponseWriter, r *http.Request) {
	//wanted := [][]string{
	//	[]string{"kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions", "Value"},
	//}
	var wanted [][]string
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&wanted); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not parse request body: " + err.Error()))
		return
	}
	ch := make(chan []metrics.Metric)
	for _, m := range wanted {
		for brokerId, _ := range metrics.BrokerUrls {
			go metrics.QueryBrokerAsync(brokerId, m[0], m[1], ch)
		}
	}
	all := make([]metrics.Metric, 0)
	for i := 0; i < len(wanted)*len(metrics.BrokerUrls); i++ {
		all = append(all, <-ch...)
	}
	close(ch)
	writeAsJson(w, all)
}
