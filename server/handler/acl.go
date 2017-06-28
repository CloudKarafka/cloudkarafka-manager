package handler

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"net/http"
)

func Acl(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		zookeeper.Acl(vars["topic"])
	case "POST":
		err := zookeeper.CreateAcl(vars["topic"], r.Body)
		if err != nil {
			internalError(w, err)
		}
		defer r.Body.Close()
	default:
		http.NotFound(w, r)
	}
}
