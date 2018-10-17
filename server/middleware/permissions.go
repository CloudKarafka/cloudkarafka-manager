package middleware

import (
	"net/http"

	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

func ClusterRead(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		p := r.Context().Value("permissions").(zookeeper.Permissions)
		if !p.ClusterRead() {
			http.NotFound(w, r)
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func ClusterWrite(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		p := r.Context().Value("permissions").(zookeeper.Permissions)
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}
