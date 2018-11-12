package api

import (
	"fmt"
	"net/http"
	"os"

	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	"goji.io/pat"
)

type user struct {
	Name, Password string
}

func Users(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	users, err := zookeeper.Users(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[INFO] api.Users: %s", err)
		http.Error(w, "Could not retrive save user in ZooKeeper", http.StatusInternalServerError)
		return
	}
	res := make([]zookeeper.Permissions, len(users))
	for i, user := range users {
		res[i] = zookeeper.PermissionsFor(user)
	}
	writeAsJson(w, res)
}

func CreateUser(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] api.CreateUser: %s", err)
		http.Error(w, "Cannot parse request body", http.StatusBadRequest)
		return
	}
	name := r.PostFormValue("name")
	pwd := r.PostFormValue("password")
	err = zookeeper.CreateUser(name, pwd)
	if err != nil {
		w.Header().Add("Content-type", "text/plain")
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	fmt.Printf("[INFO] action=create-user user=%s\n", name)
	w.WriteHeader(http.StatusCreated)
}

func User(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	user := zookeeper.PermissionsFor(name)
	writeAsJson(w, user)
}

func DeleteUser(w http.ResponseWriter, r *http.Request) {
	name := pat.Param(r, "name")
	if name != "admin" {
		zookeeper.DeleteUser(name)
		fmt.Printf("[INFO] action=delete-user user=%s\n", name)
	}
	w.WriteHeader(http.StatusNoContent)
}
