package web

import (
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func ListUsers(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListUsers() {
		return templates.ErrorRenderer(insuffcientPermissionsError)
	}
	var (
		users             []store.KafkaUser
		cacheKey          = "kafkauser_list"
		cacheEntry, found = Cache.Get(cacheKey)
	)
	if found {
		users = cacheEntry.([]store.KafkaUser)
	} else {
		users, err := store.Users(user.Permissions)
		if err != nil {
			return templates.ErrorRenderer(err)
		}
		Cache.Set(cacheKey, users, DefaultExpiration)
	}
	return templates.DefaultRenderer("users", users)
}

func CreateUser(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.CreateUser() {
		return templates.ErrorRenderer(insuffcientPermissionsError)
	}
	r.ParseForm()
	err := zookeeper.CreateUser(r.FormValue("name"), r.FormValue("password"))
	if err != nil {
		log.Error("create_user", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	log.Info("create_user", log.MapEntry{"username": r.FormValue("name")})
	http.Redirect(w, r, "/users", 302)
	return nil
}

func DeleteUser(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.CreateUser() {
		return templates.ErrorRenderer(insuffcientPermissionsError)
	}
	name := pat.Param(r, "name")
	err := zookeeper.DeleteUser(name)
	if err != nil {
		log.Error("delete_user", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	log.Info("delete_user", log.MapEntry{"username": r.FormValue("name")})
	http.Redirect(w, r, "/users", 302)
	return nil
}
