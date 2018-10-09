package server

import (
	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/server/api"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	_ "net/http/pprof"

	"fmt"
	"net/http"
	"runtime"
	"time"
)

type aclScopedHandler func(http.ResponseWriter, *http.Request, zookeeper.Permissions)

func protected(fn aclScopedHandler) http.HandlerFunc {
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
			w.Header().Set("WWW-Authenticate", "Basic realm=\"Restricted\"")
			w.WriteHeader(http.StatusUnauthorized)
		}
	}
}

func protecedServeFile(r *mux.Router, path, file string) {
	r.HandleFunc(path, protected(func(w http.ResponseWriter, req *http.Request, _ zookeeper.Permissions) {
		http.ServeFile(w, req, "static/"+file)
	}))
}

type Version struct {
	GitCommit string `json:"git_commit"`
	Version   string `json:"version"`
}

var CurrentVersion Version

func apiRoutes(r *mux.Router) {
	a := r.PathPrefix("/api").Subrouter()
	a.HandleFunc("/acls.json", protected(api.Acls))
	a.HandleFunc("/acls", protected(api.Acls))
	a.HandleFunc("/acls/{resource}/{name}/{principal}", protected(api.Acl))
	a.HandleFunc("/brokers.json", protected(api.Brokers))
	a.HandleFunc("/brokers/throughput.json", protected(api.AllBrokerThroughputTimeseries))
	a.HandleFunc("/brokers/{id}.json", protected(api.Broker))
	a.HandleFunc("/brokers/{id}/jvm.json", protected(api.BrokerJvm))
	a.HandleFunc("/brokers/{id}/health.json", protected(api.BrokerHealth))
	a.HandleFunc("/brokers/{id}/throughput.json", protected(api.BrokerThroughputTimeseries))
	a.HandleFunc("/brokers/throughput", protected(api.AllBrokerThroughputTimeseries))
	a.HandleFunc("/topics.json", protected(api.Topics))
	a.HandleFunc("/topics", protected(api.Topics))
	a.HandleFunc("/topics/{topic}.json", protected(api.Topic))
	a.HandleFunc("/topics/{topic}", protected(api.Topic))
	a.HandleFunc("/topics/{topic}/config.json", protected(api.Config))
	a.HandleFunc("/topics/{topic}/config", protected(api.Config))
	a.HandleFunc("/topics/{topic}/spread-partitions", protected(api.SpreadPartitions))
	a.HandleFunc("/topics/{topic}/throughput.json", protected(api.TopicThroughput))
	a.HandleFunc("/consumers.json", protected(api.Consumers))
	a.HandleFunc("/consumers/{name}.json", protected(api.Consumer))
	a.HandleFunc("/whoami.json", protected(api.Whoami))
	a.HandleFunc("/whoami", protected(api.Whoami))
	a.HandleFunc("/users.json", protected(api.Users))
	a.HandleFunc("/users", protected(api.Users))
	a.HandleFunc("/users/{name}.json", protected(api.User))
	a.HandleFunc("/users/{name}", protected(api.User))
	a.HandleFunc("/notifications.json", protected(api.Notifications))
	a.HandleFunc("/notifications.json", protected(api.Notifications))
	a.HandleFunc("/version", func(w http.ResponseWriter, _r *http.Request) {
		api.WriteJson(w, CurrentVersion)
	})
	a.HandleFunc("/debug/memory-usage", protected(func(w http.ResponseWriter, _r *http.Request, _p zookeeper.Permissions) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		api.WriteJson(w, map[string]string{
			"Alloc":      fmt.Sprintf("%v MiB", m.Alloc/1024/1024),
			"TotalAlloc": fmt.Sprintf("%v MiB", m.TotalAlloc/1024/1024),
			"Sys":        fmt.Sprintf("%v MiB", m.Sys/1024/1024),
			"NumGC":      fmt.Sprintf("%v", m.NumGC),
		})
	}))
}

func serveFile(r *mux.Router, path, file string) {
	r.HandleFunc(path, func(w http.ResponseWriter, req *http.Request) {
		http.ServeFile(w, req, "static/"+file)
	})
}

func Start() {
	r := mux.NewRouter()
	apiRoutes(r)

	serveFile(r, "/", "index.html")
	serveFile(r, "/topics", "topics/index.html")
	serveFile(r, "/topics/add", "topics/add.html")
	serveFile(r, "/topic/edit", "topic/edit.html")
	serveFile(r, "/topic/details", "topic/details.html")
	serveFile(r, "/brokers", "brokers.html")
	serveFile(r, "/broker/details", "broker/details.html")
	serveFile(r, "/consumers", "consumers/index.html")
	serveFile(r, "/consumer/details", "consumer/details.html")
	serveFile(r, "/admin", "admin/index.html")
	serveFile(r, "/users/add", "users/add.html")
	serveFile(r, "/acls/add", "acls/add.html")
	serveFile(r, "/login", "login.html")

	http.Handle("/js/", http.FileServer(http.Dir("static/")))
	http.Handle("/css/", http.FileServer(http.Dir("static/")))
	http.Handle("/fonts/", http.FileServer(http.Dir("static/")))
	http.Handle("/assets/", http.FileServer(http.Dir("static/")))

	http.Handle("/", r) //handlers.RecoveryHandler()(r))
	s := &http.Server{
		Addr:         fmt.Sprintf(":%s", config.Port),
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
	}
	fmt.Println("[INFO] Listening on Port", config.Port)
	fmt.Println(s.ListenAndServe())
}
