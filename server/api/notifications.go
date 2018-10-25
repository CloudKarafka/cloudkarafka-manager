package api

import (
	"github.com/84codes/cloudkarafka-mgmt/dm"

	"github.com/zenazn/goji/web"

	"net/http"
)

func init() {
	Mux.Get("/notifications", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterRead() {
			http.NotFound(w, r)
			return
		}
		WriteJson(w, dm.Notifications())
	})
}
