package server

import (
	"cloudkarafka-mgmt/server/handler"

	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"

	"log"
	"net/http"
	"os"
	"time"
)

func Start(port string) {
	go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stdout, "metrics: ", log.Lmicroseconds))

	r := mux.NewRouter()
	r.HandleFunc("/api/brokers", handler.Brokers)
	r.HandleFunc("/api/brokers/{id}", handler.Broker)
	r.HandleFunc("/api/topics", handler.Topics)
	r.HandleFunc("/api/topics/{name}", handler.Topic)
	r.HandleFunc("/api/consumers", handler.Consumers)
	r.HandleFunc("/api/consumers/{name}", handler.Consumer)
	http.Handle("/", r)
	s := &http.Server{
		Addr:         port,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	s.ListenAndServe()
}
