package web

import (
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func ListACLs(w http.ResponseWriter, r *http.Request) templates.Result {
	aclType := pat.Param(r, "type")
	var (
		data  []zookeeper.ACLRule
		cData zookeeper.ACLRule
		err   error
	)
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	switch aclType {
	case "cluster":
		cData, err = zookeeper.ClusterAcls(p)
		data = []zookeeper.ACLRule{cData}
	case "group":
		data, err = zookeeper.GroupAcls(p)
	default:
		data, err = zookeeper.TopicAcls(p)
	}
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	return templates.DefaultRenderer("acls", data)
}
