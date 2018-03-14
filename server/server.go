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
	a.HandleFunc("/acls.json", ah(api.Acls))
	a.HandleFunc("/acls/{type}/{resource}/{username}.json", ah(api.Acl))
	a.HandleFunc("/brokers.json", ah(api.Brokers))
	a.HandleFunc("/brokers/{id}.json", ah(api.Broker))
	a.HandleFunc("/brokers/{id}/metrics.json", ah(api.BrokerMetrics))
	a.HandleFunc("/topics.json", ah(api.Topics))
	a.HandleFunc("/topics/{topic}.json", ah(api.Topic))
	a.HandleFunc("/topics/{topic}/metrics.json", ah(api.TopicMetrics))
	a.HandleFunc("/topics/{topic}/config.json", ah(api.Config))
	a.HandleFunc("/topics/{topic}/reassigning.json", ah(api.ReassigningTopic))
	a.HandleFunc("/consumers.json", ah(api.Consumers))
	a.HandleFunc("/consumers/{name}.json", ah(api.Consumer))
	a.HandleFunc("/consumers/{name}/metrics.json", ah(api.ConsumerMetrics))
	a.HandleFunc("/whoami.json", ah(api.Whoami))
	a.HandleFunc("/users.json", ah(api.Users))
	a.HandleFunc("/users/{name}.json", ah(api.User))
}

func Start(cert, key string) {
	r := mux.NewRouter()
	apiRoutes(r)

	r.HandleFunc("/", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/index.html")
	}))

	r.HandleFunc("/api", func(w http.ResponseWriter, req *http.Request) {
		http.ServeFile(w, req, "static/html/docs.html")
	})

	r.HandleFunc("/topics", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/topics/index.html")
	}))

	r.HandleFunc("/topics/details", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/topic/details.html")
	}))

	r.HandleFunc("/brokers", ah(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/brokers.html")
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
	http.Handle("/assets/", http.FileServer(http.Dir("static/")))
	http.Handle("/", r)
	s := &http.Server{
		Addr:         fmt.Sprintf(":%s", config.Port),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	fmt.Println("Listening on Port", config.Port)
	fmt.Println(s.ListenAndServe())
}
