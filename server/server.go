package server

import (
	"log"

	"github.com/84codes/cloudkarafka-mgmt/config"

	"github.com/84codes/cloudkarafka-mgmt/server/api"
	"github.com/84codes/cloudkarafka-mgmt/server/debug"

	_ "net/http/pprof"

	"fmt"
	"net/http"

	goji "goji.io"
	"goji.io/pat"
)

func Start() {
	root := goji.NewMux()
	root.Handle(pat.New("/api/*"), api.Router())
	//root.Handle(pat.New("/web/*"), web.Router())
	root.Handle(pat.New("/debug/*"), debug.Router())

	root.Handle(pat.Get("/*"), http.FileServer(http.Dir("static/")))

	fmt.Println("[INFO] Listening on Port", config.Port)
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", root))
}
