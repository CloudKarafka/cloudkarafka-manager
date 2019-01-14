package middleware

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func HostnameToResponse(h http.Handler) http.Handler {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = RandStringBytes(10)
		fmt.Fprintf(os.Stderr, "[INFO] Could not get hostname for response writer middleware: %s, setting hostname to %s\n", err, hostname)
	}
	fmt.Fprintf(os.Stderr, "[INFO] hostname=%s\n", hostname)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Kafka-Broker", hostname)
		h.ServeHTTP(w, r)
	})
}
