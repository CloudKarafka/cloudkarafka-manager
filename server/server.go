package server

import (
	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/server/api"

	"github.com/zenazn/goji"

	_ "net/http/pprof"

	"fmt"
	"net"
	"net/http"
)

func serveFile(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := fmt.Sprintf("./static/%s", path)
		fmt.Println(p)
		http.ServeFile(w, r, p)
	}
}

func Start() {
	goji.Handle("/api/*", api.Mux)
	goji.Get("/", serveFile("index.html"))
	goji.Get("/topics", serveFile("topics/index.html"))
	goji.Get("/topics/add", serveFile("topics/add.html"))
	goji.Get("/topic/edit", serveFile("topic/edit.html"))
	goji.Get("/topic/details", serveFile("topic/details.html"))
	goji.Get("/topic/browse", serveFile("topic/browse.html"))
	goji.Get("/brokers", serveFile("brokers.html"))
	goji.Get("/broker/details", serveFile("broker/details.html"))
	goji.Get("/consumers", serveFile("consumers/index.html"))
	goji.Get("/consumer/details", serveFile("consumer/details.html"))
	goji.Get("/admin", serveFile("admin/index.html"))
	goji.Get("/users/add", serveFile("users/add.html"))
	goji.Get("/acls/add", serveFile("acls/add.html"))
	goji.Get("/login", serveFile("login.html"))

	http.Handle("/js/", http.FileServer(http.Dir("static/")))
	http.Handle("/css/", http.FileServer(http.Dir("static/")))
	http.Handle("/fonts/", http.FileServer(http.Dir("static/")))
	http.Handle("/assets/", http.FileServer(http.Dir("static/")))

	fmt.Println("[INFO] Listening on Port", config.Port)
	listener, err := net.Listen("tcp4", fmt.Sprintf(":%s", config.Port))
	if err != nil {
		fmt.Println(err)
	}
	goji.ServeListener(listener)
}
