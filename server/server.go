package server

import (
	"cloudkarafka-mgmt/config"
	"cloudkarafka-mgmt/server/api"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"fmt"
	"net/http"
	"time"
)

func ah(fn aclScopedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if zookeeper.SkipAuthentication() {
			p := zookeeper.Permissions{Cluster: zookeeper.R, Username: "default"}
			fn(w, r, p)
			//fmt.Printf("[INFO] method=%s route=%s status=%s\n", r.Method, r.URL.Path, w.Header())
		} else if zookeeper.SkipAuthenticationWithWrite() {
			p := zookeeper.Permissions{Cluster: zookeeper.W, Username: "default"}
			fn(w, r, p)
			//fmt.Printf("[INFO] method=%s route=%s status=%s\n", r.Method, r.URL.Path, w.Header())
		} else if ok && zookeeper.ValidateScramLogin(user, pass) {
			p := zookeeper.PermissionsFor(user)
			fn(w, r, p)
			//fmt.Printf("[INFO] method=%s route=%s status=%s\n", r.Method, r.URL.Path, w.Header())
		} else {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			w.WriteHeader(http.StatusUnauthorized)
		}
	}
}

type aclScopedHandler func(http.ResponseWriter, *http.Request, zookeeper.Permissions)

func apiRoutes(r *mux.Router) {
	a := r.PathPrefix("/api").Subrouter()
	a.HandleFunc("/acls", ah(api.Acls))
	a.HandleFunc("/acls/{type}/{resource}/{username}", ah(api.Acl))
	a.HandleFunc("/brokers", ah(api.Brokers))
	a.HandleFunc("/brokers/{id}", ah(api.Broker))
	a.HandleFunc("/brokers/{id}/metrics", ah(api.BrokerMetrics))
	a.HandleFunc("/topics", ah(api.Topics))
	a.HandleFunc("/topics/{topic}", ah(api.Topic))
	a.HandleFunc("/topics/{topic}/metrics", ah(api.TopicMetrics))
	a.HandleFunc("/topics/{topic}/config", ah(api.Config))
	a.HandleFunc("/topics/{topic}/reassigning", ah(api.ReassigningTopic))
	a.HandleFunc("/topics/{topic}/{partition}", ah(api.Partition))
	a.HandleFunc("/topics/{topic}/{partition}/metrics", ah(api.PartitionMetrics))
	a.HandleFunc("/consumers", ah(api.Consumers))
	a.HandleFunc("/consumers/{name}", ah(api.Consumer))
	a.HandleFunc("/consumers/{name}/metrics", ah(api.ConsumerMetrics))
	a.HandleFunc("/whoami", ah(api.Whoami))
	a.HandleFunc("/users", ah(api.Users))
	a.HandleFunc("/users/{name}", ah(api.User))
}

func Start(cert, key string) {
	r := mux.NewRouter()
	apiRoutes(r)

	r.HandleFunc("/", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/home.html")
	}))

	r.HandleFunc("/api", func(w http.ResponseWriter, req *http.Request) {
		http.ServeFile(w, req, "static/html/docs.html")
	})

	r.HandleFunc("/topics/{topic}", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/topic.html")
	}))

	r.HandleFunc("/brokers/{id}", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/broker.html")
	}))

	r.HandleFunc("/users/{name}", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/user.html")
	}))

	r.HandleFunc("/consumers/{name}", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/consumer.html")
	}))

	http.Handle("/js/", http.FileServer(http.Dir("static/")))
	http.Handle("/css/", http.FileServer(http.Dir("static/")))
	http.Handle("/fonts/", http.FileServer(http.Dir("static/")))
	http.Handle("/", r)
	s := &http.Server{
		Addr:         fmt.Sprintf(":%s", config.Port),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	fmt.Println("Listening on Port", config.Port)
	fmt.Println(s.ListenAndServe())
}
