package middleware

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

func RequestId(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.New().String()
			r.Header.Set("X-Request-ID", rid)
		}
		ctx := context.WithValue(r.Context(), "requestId", rid)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
