package api

import (
	"encoding/json"
	"net/http"

	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	goji "goji.io"
	"goji.io/pat"
)

func writeAsJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bytes)
}

func WhoAmI(w http.ResponseWriter, r *http.Request) {
	writeAsJson(w, r.Context().Value("permissions"))
}

func Router() *goji.Mux {
	mux := goji.SubMux()
	mux.Use(m.RequestId)
	mux.Use(m.Logger)
	mux.Use(m.HostnameToResponse)
	mux.Use(m.Secure)

	mux.Handle(pat.Get("/whoami"), http.HandlerFunc(WhoAmI))
	mux.Handle(pat.Get("/notifications"), http.HandlerFunc(Notifications))
	//mux.Handle(pat.Get("/overview"), http.HandlerFunc(Overview))
	mux.Handle(pat.Get("/brokers"), http.HandlerFunc(Brokers))
	mux.Handle(pat.Get("/brokers/throughput"), http.HandlerFunc(BrokersThroughput))
	mux.Handle(pat.Get("/brokers/:id"), http.HandlerFunc(Broker))
	mux.Handle(pat.Get("/brokers/:id/throughput"), http.HandlerFunc(BrokerThroughput))

	mux.Handle(pat.Get("/consumers"), http.HandlerFunc(Consumers))
	mux.Handle(pat.Get("/consumer/:name"), http.HandlerFunc(Consumer))

	mux.Handle(pat.Get("/topics"), http.HandlerFunc(Topics))
	mux.Handle(pat.Post("/topics"), m.ClusterWrite(http.HandlerFunc(CreateTopic)))
	mux.Handle(pat.Get("/topics/:name"), http.HandlerFunc(Topic))
	mux.Handle(pat.Put("/topics/:name"), m.ClusterWrite(http.HandlerFunc(UpdateTopic)))
	mux.Handle(pat.Delete("/topics/:name"), m.ClusterWrite(http.HandlerFunc(DeleteTopic)))
	mux.Handle(pat.Get("/topics/:name/throughput"), http.HandlerFunc(TopicThroughput))
	mux.Handle(pat.Get("/topics/:name/browse"), http.HandlerFunc(TopicBrowser))

	mux.Handle(pat.Get("/users"), m.ClusterRead(http.HandlerFunc(Users)))
	mux.Handle(pat.Post("/users"), m.ClusterWrite(http.HandlerFunc(CreateUser)))
	mux.Handle(pat.Delete("/users/:name"), m.ClusterWrite(http.HandlerFunc(DeleteUser)))

	mux.Handle(pat.Get("/certificates"), m.ClusterRead(http.HandlerFunc(ListSSLCerts)))
	mux.Handle(pat.Post("/certificates"), m.ClusterWrite(http.HandlerFunc(CreateSSLCert)))
	mux.Handle(pat.Post("/certificates/sign"), m.ClusterRead(http.HandlerFunc(SignCert)))
	mux.Handle(pat.Post("/certificates/keystore/:name"), m.ClusterWrite(http.HandlerFunc(UpdateKafkaKeystore)))
	mux.Handle(pat.Post("/certificates/:alias"), m.ClusterWrite(http.HandlerFunc(ImportSSLCert)))
	mux.Handle(pat.Put("/certificates/:alias"), m.ClusterWrite(http.HandlerFunc(RenewSSLCert)))
	mux.Handle(pat.Delete("/certificates/:alias/trust"), m.ClusterWrite(http.HandlerFunc(RevokeSSLCert)))
	mux.Handle(pat.Delete("/certificates/:alias/key"), m.ClusterWrite(http.HandlerFunc(RemoveSSLKey)))

	mux.Handle(pat.Get("/acls"), m.ClusterRead(http.HandlerFunc(Acl)))
	mux.Handle(pat.Post("/acls"), m.ClusterWrite(http.HandlerFunc(CreateAcl)))
	mux.Handle(pat.Delete("/acls/:resource/:name/:principal"), m.ClusterWrite(http.HandlerFunc(DeleteAcl)))

	mux.Handle(pat.Post("/metricsbatch"), m.ClusterRead(http.HandlerFunc(KafkaMetrics))) // legacy route
	mux.Handle(pat.Post("/metrics/kafka"), m.ClusterRead(http.HandlerFunc(KafkaMetrics)))
	mux.Handle(pat.Get("/metrics/zookeeper"), m.ClusterRead(http.HandlerFunc(ZookeeperMetrics)))

	mux.Handle(pat.Get("/config/kafka"), m.ClusterRead(http.HandlerFunc(GetKafkaConfig)))
	mux.Handle(pat.Get("/config/kafka/:brokerId"), m.ClusterRead(http.HandlerFunc(GetKafkaConfigBroker)))
	mux.Handle(pat.Post("/config/kafka"), m.ClusterWrite(http.HandlerFunc(UpdateKafkaConfigAll)))
	mux.Handle(pat.Post("/config/kafka/:brokerId"), m.ClusterWrite(http.HandlerFunc(UpdateKafkaConfig)))
	return mux
}
