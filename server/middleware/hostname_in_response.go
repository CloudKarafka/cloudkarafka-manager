package middleware

import (
	"net/http"
	"os"
)

func HostnameToResponse(h http.Handler) http.Handler {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostname != "" {
			w.Header().Add("X-Kafka-Broker", hostname)
		}
		h.ServeHTTP(w, r)
	})
}
