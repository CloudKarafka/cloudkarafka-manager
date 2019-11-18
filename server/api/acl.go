package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
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
	user := r.Context().Value("user").(mw.SessionUser)
	p := user.Permissions
	res := make(map[string]interface{})
	v, err := zookeeper.ClusterAcls(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res["cluster"] = v
	v, err = zookeeper.GroupAcls(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res["group"] = v

	v, err = zookeeper.TopicAcls(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res["topic"] = v
	writeAsJson(w, res)
}

func aclRequestFromHttpRequest(r *http.Request, checkKeys bool) (zookeeper.AclRequest, error) {
	var acl map[string]string
	err := parseRequestBody(r, &acl)
	e := zookeeper.AclRequest{}
	if err != nil {
		return e, errors.New("Cannot parse request body")
	}
	resource, err := zookeeper.AclResourceFromString(acl["resource"])
	if err != nil {
		return e, err
	}
	pattern, err := zookeeper.AclPatternTypeFromString(acl["pattern"])
	if err != nil {
		return e, err
	}
	if checkKeys {
		for _, k := range []string{"name", "principal", "permission", "permission_type"} {
			if acl[k] == "" {
				return e, fmt.Errorf("Missing parameter %s", k)
			}
		}
	}
	req := zookeeper.AclRequest{
		ResourceType:   resource,
		PatternType:    pattern,
		Name:           acl["name"],
		Principal:      acl["principal"],
		Permission:     strings.ToUpper(acl["permission"]),
		PermissionType: strings.ToUpper(acl["permission_type"]),
	}
	return req, nil
}

func CreateAcl(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.CreateAcl() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	req, err := aclRequestFromHttpRequest(r, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	err = zookeeper.CreateAcl(req)
	if err != nil {
		log.Error("create_acl", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func DeleteAcl(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.DeleteAcl() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	req, err := aclRequestFromHttpRequest(r, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	err = zookeeper.DeleteAcl(req)
	if err != nil {
		log.Error("delete_acl", log.ErrorEntry{err})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
