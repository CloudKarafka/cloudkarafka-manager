package server

import (
	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"

	"github.com/cloudkarafka/cloudkarafka-manager/server/api"
	"github.com/cloudkarafka/cloudkarafka-manager/server/debug"
	"github.com/cloudkarafka/cloudkarafka-manager/server/web"

	"net/http"

	goji "goji.io"
	"goji.io/pat"
)

func Start() {
	root := goji.NewMux()
	root.Handle(pat.New("/api/*"), api.Router())
	root.Handle(pat.New("/debug/*"), debug.Router())
	root.Handle(pat.Get("/static/*"), http.StripPrefix("/static", http.FileServer(http.Dir("static/"))))

	root.Handle(pat.New("/*"), web.Router())

	log.Info("web_server", log.MapEntry{"port": config.Port})
	if err := http.ListenAndServe(":"+config.Port, root); err != nil {
		log.Error("web_server", log.MapEntry{"err": err})
	}
}
