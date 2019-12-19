package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	goji "goji.io"
	"goji.io/pat"
)

func writeAsJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if m, ok := bytes.(json.Marshaler); ok {
		j, _ := m.MarshalJSON()
		fmt.Fprintf(w, string(j))
	} else {
		json.NewEncoder(w).Encode(bytes)
	}
}

func WhoAmI(w http.ResponseWriter, r *http.Request) {
	writeAsJson(w, r.Context().Value("permissions"))
}

func Version(w http.ResponseWriter, r *http.Request) {
	writeAsJson(w, map[string]string{"version": config.Version})
}

func Router() *goji.Mux {
	mux := goji.SubMux()
	mux.Use(m.RequestId)
	mux.Use(m.Logger)
	mux.Use(m.HostnameToResponse)
	mux.Use(m.SecureApi)

	mux.Handle(pat.Get("/whoami"), http.HandlerFunc(WhoAmI))
	mux.Handle(pat.Get("/overview"), http.HandlerFunc(Overview))

	mux.Handle(pat.Get("/brokers"), http.HandlerFunc(Brokers))
	mux.Handle(pat.Get("/brokers/:id"), http.HandlerFunc(Broker))

	mux.Handle(pat.Get("/consumers"), http.HandlerFunc(ListConsumerGroups))
	mux.Handle(pat.Get("/consumers/:name"), http.HandlerFunc(ViewConsumerGroup))

	mux.Handle(pat.Get("/topics"), http.HandlerFunc(Topics))
	mux.Handle(pat.Post("/topics"), http.HandlerFunc(CreateTopic))
	mux.Handle(pat.Get("/topics/:name"), http.HandlerFunc(Topic))
	mux.Handle(pat.Patch("/topics/:name"), http.HandlerFunc(UpdateTopic))
	mux.Handle(pat.Delete("/topics/:name"), http.HandlerFunc(DeleteTopic))

	mux.Handle(pat.Get("/users"), http.HandlerFunc(Users))
	mux.Handle(pat.Post("/users"), http.HandlerFunc(CreateUser))
	mux.Handle(pat.Delete("/users/:name"), http.HandlerFunc(DeleteUser))

	mux.Handle(pat.Get("/acls"), http.HandlerFunc(Acl))
	mux.Handle(pat.Post("/acls"), http.HandlerFunc(CreateAcl))
	mux.Handle(pat.Delete("/acls/:resource/:name/:principal"), http.HandlerFunc(DeleteAcl))

	mux.Handle(pat.Post("/metricsbatch"), http.HandlerFunc(KafkaMetrics)) // legacy route
	mux.Handle(pat.Post("/metrics/kafka"), http.HandlerFunc(KafkaMetrics))
	mux.Handle(pat.Get("/metrics/zookeeper"), http.HandlerFunc(ZookeeperMetrics))

	mux.Handle(pat.Get("/config/kafka"), http.HandlerFunc(GetKafkaConfig))
	mux.Handle(pat.Get("/config/kafka/:brokerId"), http.HandlerFunc(GetKafkaConfigBroker))
	mux.Handle(pat.Post("/config/kafka"), http.HandlerFunc(UpdateKafkaConfigAll))
	mux.Handle(pat.Post("/config/kafka/:brokerId"), http.HandlerFunc(UpdateKafkaConfig))
	return mux
}
