package middleware

import (
	"net/http"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/zenazn/goji/web/mutil"
)

func Logger(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		reqID, _ := r.Context().Value("requestId").(string)
		log.Trace("start_request", log.MapEntry{
			"id":     reqID,
			"method": r.Method,
			"url":    r.URL,
		})
		lw := mutil.WrapWriter(w)
		t1 := time.Now()
		h.ServeHTTP(lw, r)
		if lw.Status() == 0 {
			lw.WriteHeader(http.StatusOK)
		}
		t2 := time.Now()
		log.Trace("end_request", log.MapEntry{
			"id":     reqID,
			"status": lw.Status(),
			"time":   t2.Sub(t1),
		})
	}
	return http.HandlerFunc(fn)
}
