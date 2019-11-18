package web

import (
	"net/http"

	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func ListACLs(w http.ResponseWriter, r *http.Request) templates.Result {
	aclType := pat.Param(r, "type")
	var (
		data []zookeeper.ACLRule
		err  error
	)
	user := r.Context().Value("user").(mw.SessionUser)
	p := user.Permissions
	switch aclType {
	case "cluster":
		data, err = zookeeper.ClusterAcls(p)
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
