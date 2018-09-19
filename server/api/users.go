package api

import (
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"encoding/json"
	"fmt"
	"net/http"
)

type user struct {
	Name, Password string
}

func Whoami(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	WriteJson(w, p)
}

func Users(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	if !p.ClusterRead() {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case "GET":
		users(w, p)
	case "POST":
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		u, err := decodeUser(r)
		if err != nil {
			internalError(w, err.Error())
		} else {
			createUser(w, u)
		}
	}
}

func User(w http.ResponseWriter, r *http.Request, p zookeeper.Permissions) {
	if !p.ClusterWrite() {
		http.NotFound(w, r)
		return
	}
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		if !p.ClusterRead() && vars["name"] != p.Username {
			http.NotFound(w, r)
			return
		}
		user := zookeeper.PermissionsFor(vars["name"])
		WriteJson(w, user)
	case "DELETE":
		fmt.Println(p.ClusterWrite())
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		zookeeper.DeleteUser(vars["name"])
		w.WriteHeader(http.StatusNoContent)
	}
}

func decodeUser(r *http.Request) (user, error) {
	var (
		u   user
		err error
	)
	switch r.Header.Get("content-type") {
	case "application/json":
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&u)
	default:
		err = r.ParseMultipartForm(512)
		u = user{Name: r.PostFormValue("name"), Password: r.PostFormValue("password")}
	}
	return u, err
}

func users(w http.ResponseWriter, p zookeeper.Permissions) {
	users, err := zookeeper.Users(p)
	if err != nil {
		internalError(w, err.Error())
	} else {
		WriteJson(w, users)
	}
}

func createUser(w http.ResponseWriter, u user) {
	err := zookeeper.CreateUser(u.Name, u.Password)
	if err != nil {
		internalError(w, err.Error())
		return
	}
}
