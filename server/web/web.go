package web

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"sort"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/metrics"
	m "github.com/84codes/cloudkarafka-mgmt/server/middleware"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
	goji "goji.io"
	"goji.io/pat"
)

func getTemplate(name string) (*template.Template, error) {
	mainTmpl := fmt.Sprintf("templates/%s.html", name)
	//t := template.New("content")
	t, err := template.ParseFiles([]string{mainTmpl}...)
	if err != nil {
		return nil, err
	}
	return t.ParseGlob("templates/partials/*.html")
}

func Topics(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	ctx, cancel := context.WithTimeout(r.Context(), config.WebRequestTimeout)
	defer cancel()
	topics, err := metrics.FetchTopicList(ctx, p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERR] could not create template: %s\n", err)
		return
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})
	t, err := getTemplate("topics")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERR] could not create template: %s\n", err)
		return
	}
	err = t.ExecuteTemplate(w, "content", topics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERR] execute template failed:  %s\n", err)
		return
	}

}

func Router() *goji.Mux {
	mux := goji.SubMux()
	mux.Use(m.RequestId)
	mux.Use(m.Logger)
	mux.Use(m.HostnameToResponse)
	mux.Use(m.Secure)
	mux.Handle(pat.New("/topics"), http.HandlerFunc(Topics))

	mux.Handle(pat.Get("/*"), http.FileServer(http.Dir("static/")))
	return mux
}
