package api

import (
	"cloudkarafka-mgmt/zookeeper"

	"github.com/gorilla/mux"

	"encoding/json"
	"net/http"
)

type user struct {
	Name, Password string
}

func Users(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		users(w)
	case "POST":
		u, err := decodeUser(r)
		if err != nil {
			internalError(w, err.Error())
		} else {
			createUser(w, u)
		}
	}
}

func User(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	switch r.Method {
	case "DELETE":
		zookeeper.DeleteUser(vars["name"])
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
		err = r.ParseForm()
		u = user{Name: r.PostForm.Get("name"), Password: r.PostForm.Get("password")}
	}
	return u, err
}

func getUser(w http.ResponseWriter, name string) {
}

func users(w http.ResponseWriter) {
	users, err := zookeeper.Users()
	if err != nil {
		internalError(w, err.Error())
	} else {
		writeJson(w, users)
	}
}

func createUser(w http.ResponseWriter, u user) {
	err := zookeeper.CreateUser(u.Name, u.Password)
	if err != nil {
		internalError(w, err.Error())
		return
	}
	getUser(w, u.Name)
}
