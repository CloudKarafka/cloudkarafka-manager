package api

import (
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"

	"net/http"
)

var (
	Mux = web.New()
)

func permissions(c web.C) zookeeper.Permissions {
	return c.Env["permissions"].(zookeeper.Permissions)
}

func Secure(c *web.C, h http.Handler) http.Handler {
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
			requestAuth(w)
		}

		c.Env["permissions"] = p
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func requestAuth(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Basic realm=\"Restricted\"")
	w.WriteHeader(http.StatusUnauthorized)
}

func init() {
	Mux.Use(middleware.SubRouter)
	Mux.Use(middleware.EnvInit)
	Mux.Use(Secure)
	Mux.Use(func(c *web.C, h http.Handler) http.Handler {
		wrap := func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/brokers" {
				r.URL.Path = "/brokers/"
			}
			h.ServeHTTP(w, r)
		}
		return http.HandlerFunc(wrap)
	})
	Mux.Handle("/brokers/*", brokersMux)
}
