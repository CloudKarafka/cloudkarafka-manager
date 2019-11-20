package web

import (
	"errors"
	"net/http"
	"os"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/server/cookie"
	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

func GetLogin(w http.ResponseWriter, r *http.Request) templates.Result {
	return templates.StandaloneRenderer("login", nil)
}

var invalidLogin = errors.New("Invalid login credentials")

func PostLogin(w http.ResponseWriter, r *http.Request) templates.Result {
	var (
		user *m.SessionUser
		err  error
	)
	session, err := cookie.Cookiestore.Get(r, "session")
	if err != nil {
		log.Error("post_login", log.ErrorEntry{err})
		return templates.StandaloneRenderer("login", err)
	}
	r.ParseForm()
	username := r.FormValue("username")
	password := r.FormValue("password")

	log.Info("login", log.MapEntry{"type": config.AuthType, "username": username})
	switch config.AuthType {
	case "scram":
		if username != "" &&
			password != "" &&
			zookeeper.ValidateScramLogin(username, password) {
			if p, err := zookeeper.PermissionsFor(username); err == nil {
				user = &m.SessionUser{
					Username:    username,
					Permissions: p,
				}
			}
		} else {
			err = invalidLogin
		}
	case "admin":
		adminPwd := os.Getenv("ADMIN_PASSWORD")
		if adminPwd == "" {
			log.Error("login failed", log.MapEntry{"auth_type": "admin", "reason": "no pwd set"})
		}
		if username == "admin" && password == os.Getenv("ADMIN_PASSWORD") {
			user = &m.SessionUser{
				Username:    "admin",
				Permissions: zookeeper.AdminPermissions,
			}
		} else {
			err = invalidLogin
		}
	case "dev":
		user = &m.SessionUser{
			Username:    username,
			Permissions: zookeeper.AdminPermissions,
		}
	}
	if user != nil {
		session.Values["user"] = user
		session.Save(r, w)
		http.Redirect(w, r, "/", http.StatusFound)
	} else {
		log.Info("post_login", log.ErrorEntry{err})
		session.AddFlash(err.Error(), "flash_error")
		err = session.Save(r, w)
		http.Redirect(w, r, "/login", 302)

	}

	return nil
}

func Logout(w http.ResponseWriter, r *http.Request) {
	session, err := cookie.Cookiestore.Get(r, "session")
	session.Values["user"] = nil
	err = session.Save(r, w)
	if err != nil {
		log.Error("logout", log.ErrorEntry{err})
	}
	http.Redirect(w, r, "/", http.StatusFound)
}
