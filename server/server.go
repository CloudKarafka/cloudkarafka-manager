package server

import (
	"cloudkarafka-mgmt/server/api"

	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"

	"log"
	"net/http"
	"os"
	"time"
)

func apiRoutes(r *mux.Router) {
	a := r.PathPrefix("/api").Subrouter()
	a.HandleFunc("/acl/{topic}", api.Acl)
	a.HandleFunc("/brokers", api.Brokers)
	a.HandleFunc("/brokers/{id}", api.Broker)
	a.HandleFunc("/topics", api.Topics)
	a.HandleFunc("/topics/{topic}", api.Topic)
	a.HandleFunc("/topics/{topic}/config", api.Config)
	a.HandleFunc("/topics/{topic}/{partition}", api.Partition)
	a.HandleFunc("/consumers", api.Consumers)
	a.HandleFunc("/consumers/{name}", api.Consumer)
}

func Start(port string) {
	go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stdout, "metrics: ", log.Lmicroseconds))

	r := mux.NewRouter()
	apiRoutes(r)
	r.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		http.ServeFile(w, req, "server/views/home.html")
	})

	r.HandleFunc("/topics/{topic}", func(w http.ResponseWriter, req *http.Request) {
		http.ServeFile(w, req, "server/views/topic.html")
	})

	http.Handle("/js/", http.FileServer(http.Dir("server/public/")))
	http.Handle("/css/", http.FileServer(http.Dir("server/public/")))
	http.Handle("/", r)
	s := &http.Server{
		Addr:         port,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	s.ListenAndServe()
}
