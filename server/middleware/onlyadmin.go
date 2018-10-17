package middleware

import (
	"net/http"

	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

func OnlyAdmin(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := "admin"
		_, pass, ok := r.BasicAuth()
		isAdmin := false
		if zookeeper.SkipAuthentication() {
			isAdmin = true
		} else if zookeeper.SkipAuthenticationWithWrite() {
			isAdmin = true
		} else if ok && zookeeper.ValidateScramLogin(user, pass) {
			isAdmin = true
		}
		if isAdmin {
			h.ServeHTTP(w, r)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	return http.HandlerFunc(fn)
}
