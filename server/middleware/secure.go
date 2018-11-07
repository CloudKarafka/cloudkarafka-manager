package middleware

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

func credentials(r *http.Request) (string, string) {
	if user, pass, ok := r.BasicAuth(); ok {
		return user, pass
	}
	q := r.URL.Query()
	r.Header.Set("Authorization", "Basic "+q["_a"][0])
	if user, pass, ok := r.BasicAuth(); ok {
		return user, pass
	}
	return "", ""
}

func Secure(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user, pass := credentials(r) // r.BasicAuth()
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
		} else if user != "" && pass != "" && zookeeper.ValidateScramLogin(user, pass) {
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
