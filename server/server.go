package server

import (
	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"

	"github.com/cloudkarafka/cloudkarafka-manager/server/api"
	"github.com/cloudkarafka/cloudkarafka-manager/server/debug"

	"net/http"
	"os"

	goji "goji.io"
	"goji.io/pat"
)

type StaticDir struct {
	d http.Dir
}

func (sd StaticDir) Open(name string) (http.File, error) {
	// Try name with added extension
	f, err := sd.d.Open(name + ".html")
	if os.IsNotExist(err) {
		// Not found, try again with name as supplied.
		if f, err := sd.d.Open(name); err == nil {
			return f, nil
		}
	}
	return f, err
}

func Start() {
	root := goji.NewMux()
	root.Handle(pat.New("/api/*"), api.Router())
	root.Handle(pat.New("/debug/*"), debug.Router())

	fs := http.FileServer(StaticDir{http.Dir("static/")})
	root.Handle(pat.Get("/*"), http.StripPrefix("/", fs))

	log.Info("web_server", log.MapEntry{"port": config.Port})
	if err := http.ListenAndServe(":"+config.Port, root); err != nil {
		log.Error("web_server", log.MapEntry{"err": err})
	}
}
