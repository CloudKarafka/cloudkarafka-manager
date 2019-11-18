package web

import (
	"encoding/gob"
	"net/http"

	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/gorilla/sessions"
	goji "goji.io"
	"goji.io/pat"
)

var (
	Cookiestore *sessions.CookieStore
)

func init() {
	templates.Load()
	Cookiestore = sessions.NewCookieStore([]byte("abc")) //os.Getenv("SESSION_KEY")))
	Cookiestore.Options = &sessions.Options{
		MaxAge:   60 * 15,
		HttpOnly: true,
	}
	gob.Register(m.SessionUser{})
}

func Router() *goji.Mux {
	secureMux := goji.SubMux()
	secureMux.Use(m.SecureWeb(Cookiestore))

	//mux.Handle(pat.New("/topics"), http.HandlerFunc(Topics))
	secureMux.Handle(pat.Get("/"), templates.TemplateHandler(Overview))

	secureMux.Handle(pat.Get("/brokers"), templates.TemplateHandler(Brokers))
	secureMux.Handle(pat.Get("/brokers/:id"), templates.TemplateHandler(Broker))

	secureMux.Handle(pat.Get("/topics"), templates.TemplateHandler(ListTopics))
	secureMux.Handle(pat.Get("/topics/:name"), templates.TemplateHandler(ViewTopic))
	secureMux.Handle(pat.Get("/create_topic"), templates.TemplateHandler(CreateTopic))
	secureMux.Handle(pat.Post("/create_topic"), templates.TemplateHandler(SaveTopic))
	secureMux.Handle(pat.Get("/topics/:name/edit"), templates.TemplateHandler(EditTopic))
	secureMux.Handle(pat.Post("/topcs/:name/edit"), templates.TemplateHandler(UpdateTopicConfig))
	secureMux.Handle(pat.Post("/topics/:name/add_partitions"), http.HandlerFunc(AddTopicPartitions))
	secureMux.Handle(pat.Post("/topics/:name/delete"), http.HandlerFunc(DeleteTopic))

	secureMux.Handle(pat.Get("/consumer_groups"), templates.TemplateHandler(ListConsumerGroups))
	secureMux.Handle(pat.Get("/consumer_groups/:name"), templates.TemplateHandler(ViewConsumerGroup))

	secureMux.Handle(pat.Get("/users"), templates.TemplateHandler(ListUsers))
	secureMux.Handle(pat.Post("/users"), templates.TemplateHandler(CreateUser))
	secureMux.Handle(pat.Post("/users/:name/delete"), templates.TemplateHandler(DeleteUser))

	secureMux.Handle(pat.Get("/acl"), http.RedirectHandler("/acl/topic", 301))
	secureMux.Handle(pat.Get("/acl/:type"), templates.TemplateHandler(ListACLs))

	secureMux.Handle(pat.Get("/throughput"), templates.JsonHandler(Throughput))
	secureMux.Handle(pat.Get("/throughput/follow"), templates.SseHandler(ThroughputFollow))

	secureMux.Handle(pat.Get("/logout"), http.HandlerFunc(Logout))

	mux := goji.SubMux()
	mux.Use(m.Logger)
	mux.Handle(pat.Get("/login"), templates.TemplateHandler(GetLogin))
	mux.Handle(pat.Post("/login"), templates.TemplateHandler(PostLogin))

	mux.Handle(pat.Get("/th_test"), templates.JsonHandler(Throughput))

	mux.Handle(pat.New("/*"), secureMux)
	return mux
}
