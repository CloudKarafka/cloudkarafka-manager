package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func Acls(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListAcls() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var (
		aclRules zookeeper.ACLRules
		err      error
	)
	aclRules, err = zookeeper.Acls(user.Permissions)
	if err != nil {
		jsonError(w, err.Error())
		return
	}

	// filter all rules without any users
	ars := make(zookeeper.ACLRules, 0, len(aclRules))
	for _, ar := range aclRules {
		if len(ar.Users) != 0 {
			ars = append(ars, ar)
		}
	}
	ps, p, err := pageInfo(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeAsJson(w, Page(ps, p, ars))
}

func Acl(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.ListAcls() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	var (
		p = user.Permissions
		n = pat.Param(r, "resourceName")
		t = strings.ToLower(pat.Param(r, "type"))
	)
	data, err := zookeeper.Acl(p, t, n)
	if err != nil {
		jsonError(w, err.Error())
		return
	}
	if strings.HasSuffix(r.URL.Path, "users") {
		writeAsJson(w, data.Users)
	} else {
		writeAsJson(w, data)
	}
}

func CreateAcl(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	if !user.Permissions.CreateAcl() {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	req, err := aclRequestFromHttpRequest(r, true)
	if err != nil {
		jsonError(w, err.Error())
		return
	}
	err = zookeeper.CreateAcl(req)
	if err != nil {
		log.Error("create_acl", log.ErrorEntry{err})
		jsonError(w, err.Error())
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
		log.Error("delete_acl", log.ErrorEntry{err})
		jsonError(w, err.Error())
		return
	}
	err = zookeeper.DeleteAcl(req)
	if err != nil {
		log.Error("delete_acl", log.ErrorEntry{err})
		jsonError(w, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func aclRequestFromHttpRequest(r *http.Request, checkKeys bool) (zookeeper.AclRequest, error) {
	var acl map[string]string
	err := parseRequestBody(r, &acl)
	e := zookeeper.AclRequest{}
	if err != nil {
		return e, errors.New("Cannot parse request body")
	}
	resource, err := zookeeper.AclResourceFromString(acl["resource_type"])
	if err != nil {
		return e, err
	}
	pattern, err := zookeeper.AclPatternTypeFromString(acl["pattern_type"])
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
		Principal:      fmt.Sprintf("User:%s", acl["principal"]),
		Permission:     strings.ToUpper(acl["permission"]),
		PermissionType: strings.ToUpper(acl["permission_type"]),
		Host:           acl["host"],
	}
	return req, nil
}
