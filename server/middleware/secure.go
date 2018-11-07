package middleware

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

func Secure(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		var p zookeeper.Permissions
		if zookeeper.SkipAuthentication() {
			p = zookeeper.Permissions{Cluster: zookeeper.R, Username: "default"}
		} else if zookeeper.SkipAuthenticationWithWrite() {
			p = zookeeper.Permissions{
				Cluster:  zookeeper.W,
				Topics:   map[string]zookeeper.Permission{"*": zookeeper.RW},
				Groups:   map[string]zookeeper.Permission{"*": zookeeper.RW},
				Username: "default",
			}
		} else if ok && zookeeper.ValidateScramLogin(user, pass) {
			p = zookeeper.PermissionsFor(user)
		} else {
			fmt.Fprintf(os.Stderr, "[INFO] Failed login for user %s\n", user)
			http.Error(w, "Not authorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), "permissions", p)
		h.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}
