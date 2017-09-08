package server

import (
	"cloudkarafka-mgmt/server/api"
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"fmt"
	"net/http"
	"time"
)

func ah(fn aclScopedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		if zookeeper.ValidateScramLogin(user, pass) {
			p := zookeeper.PermissionsFor(user)
			fn(w, r, p)
			fmt.Printf("[INFO] method=%s route=%s status=%s\n", r.Method, r.URL.Path, w.Header())
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
	a.HandleFunc("/acls/{topic}", ah(api.Acl))
	a.HandleFunc("/acls/{type}/{resource}/{username}", ah(api.Acl))
	a.HandleFunc("/brokers", ah(api.Brokers))
	a.HandleFunc("/brokers/{id}", ah(api.Broker))
	a.HandleFunc("/brokers/{id}/metrics", ah(api.BrokerMetrics))
	a.HandleFunc("/topics", ah(api.Topics))
	a.HandleFunc("/topics/{topic}", ah(api.Topic))
	a.HandleFunc("/topics/{topic}/metrics", ah(api.TopicMetrics))
	a.HandleFunc("/topics/{topic}/config", ah(api.Config))
	a.HandleFunc("/topics/{topic}/{partition}", ah(api.Partition))
	a.HandleFunc("/consumers", ah(api.Consumers))
	a.HandleFunc("/consumers/{name}", ah(api.Consumer))
	a.HandleFunc("/users", ah(api.Users))
	a.HandleFunc("/users/{name}", ah(api.User))
}

func Start(port, cert, key string) {
	r := mux.NewRouter()
	apiRoutes(r)

	r.HandleFunc("/", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/home.html")
	}))

	r.HandleFunc("/topics/{topic}", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/topic.html")
	}))

	r.HandleFunc("/brokers/{id}", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/broker.html")
	}))

	r.HandleFunc("/users/{name}", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/html/user.html")
	}))

	http.Handle("/js/", http.FileServer(http.Dir("static/")))
	http.Handle("/css/", http.FileServer(http.Dir("static/")))
	http.Handle("/fonts/", http.FileServer(http.Dir("static/")))
	http.Handle("/", r)
	s := &http.Server{
		Addr:         port,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	fmt.Println("Listening on Port", port)
	fmt.Println(s.ListenAndServe())
}
