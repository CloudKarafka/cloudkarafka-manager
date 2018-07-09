package api

import (
	"cloudkarafka-mgmt/dm"
	"cloudkarafka-mgmt/zookeeper"

	"net/http"
)

func Notifications(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	switch r.Method {
	case "GET":
		var data interface{}
		if s.ClusterRead() {
			data = dm.Notifications()
		}
		writeJson(w, data)
	default:
		http.NotFound(w, r)
	}
}
