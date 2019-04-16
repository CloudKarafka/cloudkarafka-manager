package middleware

import (
	"context"
	"math/rand"
	"net/http"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyz"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func RequestId(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = randString(5)
			r.Header.Set("X-Request-ID", rid)
		}
		ctx := context.WithValue(r.Context(), "requestId", rid)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
