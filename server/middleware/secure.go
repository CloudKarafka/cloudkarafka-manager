package middleware

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"github.com/gorilla/sessions"
)

type SessionUser struct {
	Username    string
	Permissions zookeeper.Permissions
}

var AnonSessionUser = SessionUser{
	Username: "Anonymous",
}

func SecureApi(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			http.Error(w, "Not authorized", http.StatusUnauthorized)
			return
		}
		var (
			user *SessionUser
		)
		switch config.AuthType {
		case "dev":
			user = &SessionUser{
				Username:    "dev",
				Permissions: zookeeper.AdminPermissions,
			}
		case "admin":
			if username == "admin" && password == os.Getenv("ADMIN_PASSWORD") {
				user = &SessionUser{
					Username:    "admin",
					Permissions: zookeeper.AdminPermissions,
				}
			} else {
				http.Error(w, "Not authorized", http.StatusUnauthorized)
				return

			}
		case "scram":
			if username != "" && password != "" && zookeeper.ValidateScramLogin(username, password) {
				p, err := zookeeper.PermissionsFor(username)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] Secure middleware: %s\n", err)
					http.Error(w, "Couldn't get user info from Zookeeper", http.StatusInternalServerError)
					return
				}
				user = &SessionUser{
					Username:    username,
					Permissions: p,
				}
			} else {
				http.Error(w, "Not authorized", http.StatusUnauthorized)
				return
			}
		}
		ctx := context.WithValue(r.Context(), "user", user)
		h.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}

func SecureWeb(store *sessions.CookieStore) func(h http.Handler) http.Handler {
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
				h.ServeHTTP(w, r.WithContext(ctx))
			} else {
				http.Redirect(w, r, "/login", 302)
			}
		}
		return http.HandlerFunc(fn)
	}
}
