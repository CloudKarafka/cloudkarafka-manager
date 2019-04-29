package web

import (
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

func GetLogin(w http.ResponseWriter, r *http.Request) templates.Result {
	return templates.StandaloneRenderer("login", nil)
}

func PostLogin(w http.ResponseWriter, r *http.Request) templates.Result {
	session, err := Cookiestore.Get(r, "session")
	if err != nil {
		log.Error("post_login", log.ErrorEntry{err})
		return templates.StandaloneRenderer("login", err)
	}
	r.ParseForm()
	username := r.FormValue("username")
	user := &m.SessionUser{
		Username:    username,
		Permissions: zookeeper.AdminPermissions,
		Active:      true,
	}
	session.Values["user"] = user
	err = session.Save(r, w)
	if err != nil {
		log.Error("post_login", log.ErrorEntry{err})
		return templates.StandaloneRenderer("login", err)
	}
	http.Redirect(w, r, "/", http.StatusFound)
	return nil
}
