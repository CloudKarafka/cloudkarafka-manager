package middleware

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"github.com/gorilla/sessions"
)

func credentials(r *http.Request) (string, string) {
	if user, pass, ok := r.BasicAuth(); ok {
		return user, pass
	}
	q := r.URL.Query()
	if v, ok := q["_a"]; ok {
		r.Header.Set("Authorization", "Basic "+v[0])
		if user, pass, ok := r.BasicAuth(); ok {
			return user, pass
		}
	}
	return "", ""
}

func Secure(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user, pass := credentials(r)
		var (
			p   zookeeper.Permissions
			err error
		)
		if zookeeper.SkipAuthenticationWithWrite() {
			user = "admin"
			p = zookeeper.AdminPermissions
		} else if user != "" && pass != "" && zookeeper.ValidateScramLogin(user, pass) {
			p, err = zookeeper.PermissionsFor(user)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ERROR] Secure middleware: %s\n", err)
				http.Error(w, "Couldn't get user info from Zookeeper", http.StatusInternalServerError)
				return
			}
		} else {
			fmt.Fprintf(os.Stderr, "[INFO] Failed login for user %s\n", user)
			http.Error(w, "Not authorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), "permissions", p)
		ctx = context.WithValue(ctx, "username", user)
		h.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}

type SessionUser struct {
	Username    string
	Permissions zookeeper.Permissions
	Active      bool
}

func SessionSecure(store *sessions.CookieStore) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			session, err := store.Get(r, "session")
			if err != nil {
				log.Error("session_secure", log.ErrorEntry{err})
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if user, ok := session.Values["user"].(SessionUser); ok {
				ctx := context.WithValue(r.Context(), "user", user)
				ctx = context.WithValue(ctx, "permissions", user.Permissions)
				h.ServeHTTP(w, r.WithContext(ctx))
			} else {
				http.Error(w, "Not authorized", http.StatusUnauthorized)
			}

		}
		return http.HandlerFunc(fn)
	}
}
