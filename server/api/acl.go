package api

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"net/http"
)

func Acl(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	vars := mux.Vars(r)
	switch r.Method {
	case "GET":
		zookeeper.TopicAcl(vars["topic"])
	case "POST":
		err := zookeeper.CreateAcl(vars["topic"], r.Body)
		if err != nil {
			internalError(w, err.Error())
		}
		defer r.Body.Close()
	default:
		http.NotFound(w, r)
	}
}
