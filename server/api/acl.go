package api

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"encoding/json"
	"net/http"
)

type aclVM struct {
	Username   string               `json:"username"`
	Resource   string               `json:"resource"`
	Type       string               `json:"type"`
	Permission zookeeper.Permission `json:"permission"`
}

func Acls(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	switch r.Method {
	case "POST":
		acl, err := decodeAcl(r)
		if err != nil {
			internalError(w, err)
			return
		}
		err = zookeeper.CreateAcl(acl.Username, acl.Resource, acl.Type, acl.Permission)
		if err != nil {
			internalError(w, err)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}

}

func Acl(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	vars := mux.Vars(r)
	switch r.Method {
	/*case "GET":
	switch vars["type"] {
	case "Cluster":
	case "Group":
		zookeeper.TopicAcl(vars["topic"])
	case "Topic":
		zookeeper.TopicAcl(vars["topic"])
	}*/
	case "DELETE":
		err := zookeeper.DeleteAcl(vars["username"], vars["resource"], vars["type"])
		if err != nil {
			internalError(w, err)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func decodeAcl(r *http.Request) (aclVM, error) {
	var (
		acl aclVM
		err error
	)
	switch r.Header.Get("content-type") {
	case "application/json":
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&acl)
		defer r.Body.Close()
	default:
		err = r.ParseForm()
		if err == nil {
			acl.Username = r.PostForm.Get("username")
			acl.Resource = r.PostForm.Get("resource")
			acl.Type = r.PostForm.Get("type")
			acl.Permission = zookeeper.ParsePermission(r.PostForm.Get("permission"))
		}
	}
	return acl, err
}
