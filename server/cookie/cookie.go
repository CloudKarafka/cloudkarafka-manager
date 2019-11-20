package cookie

import (
	"encoding/gob"
	"os"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/gorilla/sessions"
)

var (
	Cookiestore *sessions.CookieStore
)

func Setup() {
	if config.DevMode {
		Cookiestore = sessions.NewCookieStore(
			[]byte("xxx"),
		)
	} else {
		Cookiestore = sessions.NewCookieStore(
			[]byte(os.Getenv("SESSION_AUTH_KEY")),
			[]byte(os.Getenv("SESSION_ENC_KEY")),
		)
	}
	Cookiestore.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   60 * 60 * 24,
		Secure:   !config.DevMode,
		HttpOnly: true,
	}
	gob.Register(m.SessionUser{})
}
