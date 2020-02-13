package api

import (
	"fmt"
	"net/http"
	"os"

	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

type user struct {
	Name, Password string
}

func Users(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListUsers() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	users, err := zookeeper.Users(user.Username, user.Permissions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] api.Users: %s", err)
		http.Error(w, "Could not retrive user in ZooKeeper", http.StatusInternalServerError)
		return
	}
	/*
		res := make([]zookeeper.Permissions, len(users))
		for i, user := range users {
			res[i] = zookeeper.PermissionsFor(user)
		}
	*/

	ps, p, err := pageInfo(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeAsJson(w, Page(ps, p, users))
}

func CreateUser(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.AlterConfigsCluster() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	form := make(map[string]string)
	err := parseRequestBody(r, &form)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateUser: %s", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	err = zookeeper.CreateUser(form["name"], form["password"])
	if err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	fmt.Printf("[INFO] action=create-user user=%s\n", form["name"])
	w.WriteHeader(http.StatusCreated)
}

func User(w http.ResponseWriter, r *http.Request) {
	u := r.Context().Value("user").(mw.SessionUser)
	if !u.Permissions.AlterConfigsCluster() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	name := pat.Param(r, "name")
	user, err := zookeeper.PermissionsFor(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.User: %s", err)
		http.Error(w, "Couldn't get info from zookeeper", http.StatusInternalServerError)
		return
	}
	writeAsJson(w, user)
}

func DeleteUser(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.AlterConfigsCluster() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	name := pat.Param(r, "name")
	if name != "admin" {
		zookeeper.DeleteUser(name)
		fmt.Printf("[INFO] action=delete-user user=%s\n", name)
	}
	w.WriteHeader(http.StatusNoContent)
}
