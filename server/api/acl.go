package api

import (
	"cloudkarafka-mgmt/zookeeper"
	"github.com/gorilla/mux"

	"encoding/json"
	"net/http"
)

type aclVM struct {
	Principal  string               `json:"principal"`
	Name       string               `json:"name"`
	Resource   string               `json:"resource"`
	Type       string               `json:"type"`
	Host       string               `json:"host"`
	Permission zookeeper.Permission `json:"permission"`
}

func Acls(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	switch r.Method {
	case "GET":
		topics := zookeeper.AllAcls(zookeeper.TopicsAcls, zookeeper.TopicAcl)
		groups := zookeeper.AllAcls(zookeeper.GroupsAcls, zookeeper.GroupAcl)
		cluster := zookeeper.AllAcls(zookeeper.ClusterAcls, zookeeper.ClusterAcl)
		resp := map[string]interface{}{
			"topics":  topics,
			"groups":  groups,
			"cluster": cluster,
		}
		writeJson(w, resp)
	case "POST":
		acl, err := decodeAcl(r)
		if err != nil {
			internalError(w, err)
			return
		}
		err = zookeeper.CreateAcl(acl.Principal,
			acl.Name,
			acl.Resource,
			acl.Type,
			acl.Host,
			acl.Permission.String(),
		)
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
	case "DELETE":
		err := zookeeper.DeleteAcl(vars["principal"], vars["name"], vars["resource"])
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
		err = r.ParseMultipartForm(512)
		if err == nil {
			acl.Principal = r.PostForm.Get("principal")
			acl.Name = r.PostForm.Get("name")
			acl.Resource = r.PostForm.Get("resource")
			acl.Type = r.PostForm.Get("type")
			acl.Host = r.PostForm.Get("host")
			acl.Permission = zookeeper.ParsePermission(r.PostForm.Get("permission"))
		}
	}
	return acl, err
}
