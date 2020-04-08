package api

import (
	"context"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	n "github.com/cloudkarafka/cloudkarafka-manager/notifications"
)

type notification struct {
	Type    string `json:"type"`
	Level   string `json:"level"`
	Message string `json:"message"`
}
type notificationFn func()
var (
	fns = []notificationFn{
	}
)
func Notifications(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	writeAsJson(w, n.List(ctx))
}
