package web

import (
	"encoding/gob"
	"errors"
	"net/http"

	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/gorilla/securecookie"
	"github.com/gorilla/sessions"
	goji "goji.io"
	"goji.io/pat"
)

var (
	badRequest  = errors.New("Bad request")
	Cookiestore *sessions.CookieStore
)

func init() {
	templates.Load()

	authKeyOne := securecookie.GenerateRandomKey(64)
	encryptionKeyOne := securecookie.GenerateRandomKey(32)
	Cookiestore = sessions.NewCookieStore(authKeyOne, encryptionKeyOne)
	Cookiestore.Options = &sessions.Options{
		MaxAge:   60 * 15,
		HttpOnly: true,
	}
	gob.Register(m.SessionUser{})
}

func Router() *goji.Mux {
	secureMux := goji.SubMux()
	secureMux.Use(m.SessionSecure(Cookiestore))

	//mux.Handle(pat.New("/topics"), http.HandlerFunc(Topics))
	secureMux.Handle(pat.Get("/"), templates.TemplateHandler(Overview))

	secureMux.Handle(pat.Get("/brokers"), templates.TemplateHandler(Brokers))
	secureMux.Handle(pat.Get("/brokers/:id"), templates.TemplateHandler(Broker))

	secureMux.Handle(pat.Get("/topics"), templates.TemplateHandler(ListTopics))
	secureMux.Handle(pat.Get("/create_topic"), templates.TemplateHandler(CreateTopic))
	secureMux.Handle(pat.Post("/create_topic"), templates.TemplateHandler(SaveTopic))
	secureMux.Handle(pat.Get("/edit_topic/:name"), templates.TemplateHandler(ListTopics))
	secureMux.Handle(pat.Post("/edit_topic/:name"), templates.TemplateHandler(SaveTopic))

	secureMux.Handle(pat.Get("/topics/:name"), templates.TemplateHandler(ViewTopic))

	secureMux.Handle(pat.Get("/admin"), templates.TemplateHandler(ListUsers))
	secureMux.Handle(pat.Get("/admin/users"), templates.TemplateHandler(ListUsers))
	secureMux.Handle(pat.Get("/admin/acl"), http.RedirectHandler("/admin/acl/topic", 301))
	secureMux.Handle(pat.Get("/admin/acl/:type"), templates.TemplateHandler(ListACLs))

	secureMux.Handle(pat.Get("/throughput"), templates.JsonHandler(Throughput))
	secureMux.Handle(pat.Get("/throughput/follow"), templates.SseHandler(ThroughputFollow))

	mux := goji.SubMux()
	mux.Use(m.Logger)
	mux.Handle(pat.Get("/login"), templates.TemplateHandler(GetLogin))
	mux.Handle(pat.Post("/login"), templates.TemplateHandler(PostLogin))
	mux.Handle(pat.New("/*"), secureMux)
	return mux
}
