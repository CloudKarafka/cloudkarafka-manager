package api

import (
	"cloudkarafka-mgmt/dm"
	"cloudkarafka-mgmt/zookeeper"

	"net/http"
)

func Notifications(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	switch r.Method {
	case "GET":
		writeJson(w, dm.Notifications())
	default:
		http.NotFound(w, r)
	}
}
