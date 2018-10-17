package debug

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"

	"github.com/84codes/cloudkarafka-mgmt/config"
	mw "github.com/84codes/cloudkarafka-mgmt/server/middleware"
	goji "goji.io"
	"goji.io/pat"
)

func writeAsJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bytes)
}

func Router() *goji.Mux {
	mux := goji.SubMux()
	mux.Use(mw.OnlyAdmin)
	mux.Handle(pat.Get("/memory-usage"), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		writeAsJson(w, map[string]string{
			"Alloc":       fmt.Sprintf("%v MiB", m.Alloc/1024/1024),
			"TotalAlloc":  fmt.Sprintf("%v MiB", m.TotalAlloc/1024/1024),
			"Sys":         fmt.Sprintf("%v MiB", m.Sys/1024/1024),
			"NumGC":       fmt.Sprintf("%v", m.NumGC),
			"NumRoutines": fmt.Sprintf("%d", runtime.NumGoroutine()),
		})
	}))

	mux.Handle(pat.Get("/version"), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeAsJson(w, map[string]string{
			"version":    config.Version,
			"git_commit": config.GitCommit,
		})
	}))
	return mux
}
