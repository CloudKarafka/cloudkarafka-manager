package api

import (
	"fmt"

	"github.com/84codes/cloudkarafka-mgmt/zookeeper"

	"github.com/zenazn/goji/web"

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

func init() {
	Mux.Get("/acls", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterRead() {
			http.NotFound(w, r)
			return
		}
		topics := zookeeper.AllAcls(zookeeper.TopicsAcls, zookeeper.TopicAcl)
		groups := zookeeper.AllAcls(zookeeper.GroupsAcls, zookeeper.GroupAcl)
		cluster := zookeeper.AllAcls(zookeeper.ClusterAcls, zookeeper.ClusterAcl)
		resp := map[string]interface{}{
			"topics":  topics,
			"groups":  groups,
			"cluster": cluster,
		}
		fmt.Printf("[INFO] action=list-acl user=%s\n", p.Username)
		WriteJson(w, resp)
	})

	Mux.Post("/acls", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterRead() {
			http.NotFound(w, r)
			return
		}
		acl, err := decodeAcl(r)
		if err != nil {
			internalError(w, err)
			return
		}
		fmt.Printf("[INFO] action=create-acl user=%s acl=[resource=%s,name=%s,principal=%s]\n",
			p.Username, acl.Resource, acl.Name, acl.Principal)
		err = zookeeper.CreateAcl(acl.Principal,
			acl.Name,
			acl.Resource,
			acl.Type,
			acl.Host,
			acl.Permission.String(),
		)
		if err != nil {
			internalError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	Mux.Delete("/acls/:resource/:name/:principal", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		fmt.Printf("[INFO] action=delete-acl user=%s acl=[resource=%s,name=%s,principal=%s]\n",
			p.Username, c.URLParams["resource"], c.URLParams["name"], c.URLParams["principal"])
		err := zookeeper.DeleteAcl(c.URLParams["principal"], c.URLParams["name"], c.URLParams["resource"])
		if err != nil {
			internalError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
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
