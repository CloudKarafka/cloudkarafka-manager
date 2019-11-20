package cookie

import (
	"encoding/gob"
	"log"
	"os"

	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/gorilla/sessions"
)

var (
	Cookiestore *sessions.CookieStore
)

func init() {
	authKey := os.Getenv("SESSION_AUTH_KEY")
	encKey := os.Getenv("SESSION_ENC_KEY")
	if authKey == "" {
		log.Panic("No env SESSION_AUTH_KEY found")
	}
	if encKey == "" {
		Cookiestore = sessions.NewCookieStore(
			[]byte(authKey),
		)

	} else {
		Cookiestore = sessions.NewCookieStore(
			[]byte(authKey),
			[]byte(encKey),
		)

	}
	Cookiestore.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   60 * 60 * 24,
		HttpOnly: true,
	}
	gob.Register(m.SessionUser{})
}
