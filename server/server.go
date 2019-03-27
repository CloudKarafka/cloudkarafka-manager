package server

import (
	"log"

	"github.com/cloudkarafka/cloudkarafka-manager/config"

	"github.com/cloudkarafka/cloudkarafka-manager/server/api"
	"github.com/cloudkarafka/cloudkarafka-manager/server/debug"
	"github.com/cloudkarafka/cloudkarafka-manager/server/web"

	"fmt"
	"net/http"

	goji "goji.io"
	"goji.io/pat"
)

func Start() {
	root := goji.NewMux()
	root.Handle(pat.New("/api/*"), api.Router())
	root.Handle(pat.New("/debug/*"), debug.Router())
	root.Handle(pat.New("/metrics"), http.HandlerFunc(api.Prometheus))

	root.Handle(pat.New("/*"), web.Router())

	root.Handle(pat.Get("/*"), http.FileServer(http.Dir("static/")))
	fmt.Println("[INFO] Listening on Port", config.Port)
	log.Fatal(http.ListenAndServe(":"+config.Port, root))
}
