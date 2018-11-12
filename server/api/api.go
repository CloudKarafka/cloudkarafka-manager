package api

import (
	"encoding/json"
	"net/http"

	"github.com/84codes/cloudkarafka-mgmt/db"
	m "github.com/84codes/cloudkarafka-mgmt/server/middleware"
	bolt "go.etcd.io/bbolt"
	goji "goji.io"
	"goji.io/pat"
)

func writeAsJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bytes)
}

func All(w http.ResponseWriter, r *http.Request) {
	var res = map[string]interface{}{
		"groups":  nil,
		"topics":  nil,
		"brokers": nil,
	}
	err := db.View(func(tx *bolt.Tx) error {
		for k, _ := range res {
			b := db.BucketByPath(tx, k)
			if b != nil {
				res[k] = db.Recur(b, 100)
			}
		}
		writeAsJson(w, res)
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func WhoAmI(w http.ResponseWriter, r *http.Request) {
	writeAsJson(w, r.Context().Value("permissions"))
}

func Router() *goji.Mux {
	mux := goji.SubMux()
	mux.Use(m.RequestId)
	mux.Use(m.Logger)
	mux.Use(m.Secure)

	mux.Handle(pat.Get("/whoami"), http.HandlerFunc(WhoAmI))
	mux.Handle(pat.Get("/notifications"), http.HandlerFunc(Notifications))
	mux.Handle(pat.Get("/overview"), http.HandlerFunc(Overview))
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

	mux.Handle(pat.Get("/acls"), m.ClusterRead(http.HandlerFunc(Acl)))
	mux.Handle(pat.Post("/acls"), m.ClusterWrite(http.HandlerFunc(CreateAcl)))
	mux.Handle(pat.Delete("/acls/:resource/:name/:principal"), m.ClusterWrite(http.HandlerFunc(DeleteAcl)))

	mux.Handle(pat.Get("/dump"), m.ClusterRead(http.HandlerFunc(All)))
	return mux
}