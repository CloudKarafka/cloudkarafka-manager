package api

import (
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

type aclVM struct {
	Principal  string               `json:"principal"`
	Name       string               `json:"name"`
	Resource   string               `json:"resource"`
	Type       string               `json:"type"`
	Host       string               `json:"host"`
	Permission zookeeper.Permission `json:"permission"`
}

func Acl(w http.ResponseWriter, r *http.Request) {
	/*
		topics := zookeeper.AllAcls(zookeeper.TopicsAcls, zookeeper.TopicAcl)
		groups := zookeeper.AllAcls(zookeeper.GroupsAcls, zookeeper.GroupAcl)
		cluster := zookeeper.AllAcls(zookeeper.ClusterAcls, zookeeper.ClusterAcl)
		resp := map[string]interface{}{
			"topics":  topics,
			"groups":  groups,
			"cluster": cluster,
		}
		writeAsJson(w, resp)
	*/
}

func CreateAcl(w http.ResponseWriter, r *http.Request) {
	/*
		p := r.Context().Value("permissions").(zookeeper.Permissions)
		if !p.ClusterRead() {
			http.NotFound(w, r)
			return
		}
		var acl aclVM
		err := parseRequestBody(r, &acl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] api.CreateAcl: %s", err)
			http.Error(w, "Cannot parse request body", http.StatusBadRequest)
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
			fmt.Fprintf(os.Stderr, "[ERROR] api.CreateAcl: %s\n", err)
			http.Error(w, "Could not save ACL rule", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	*/
}

func DeleteAcl(w http.ResponseWriter, r *http.Request) {
	/*
		p := r.Context().Value("permissions").(zookeeper.Permissions)
		resource := pat.Param(r, "resource")
		name := pat.Param(r, "name")
		principal := pat.Param(r, "principal")
		if !strings.HasPrefix(principal, "User:") {
			principal = "User:" + principal
		}
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(os.Stderr, "[INFO] action=delete-acl user=%s acl=[resource=%s,name=%s,principal=%s]\n",
			p.Username, resource, name, principal)
		err := zookeeper.DeleteAcl(principal, name, resource)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] api.DeleteAcl: %s", err)
			http.Error(w, "Could not delete ACL rule", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	*/
}
