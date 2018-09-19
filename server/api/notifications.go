package api

import (
	"github.com/84codes/cloudkarafka-mgmt/dm"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"

	"net/http"
)

func Notifications(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	switch r.Method {
	case "GET":
		var data interface{}
		if s.ClusterRead() {
			data = dm.Notifications()
		}
		WriteJson(w, data)
	default:
		http.NotFound(w, r)
	}
}
