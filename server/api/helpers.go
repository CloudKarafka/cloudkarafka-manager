package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

func parseRequestBody(r *http.Request, target interface{}) error {
	switch r.Header.Get("content-type") {
	case "application/json":
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()
		if err := decoder.Decode(target); err != nil {
			return errors.New("Could not parse request body")
		}
		return nil
	}
	return errors.New("This endpoint only supports content type application/json")
}

func SSEHeaders(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
}

func SetupSSE(retry int, rw http.ResponseWriter) {
	SSEHeaders(rw)
	fmt.Fprint(rw, "\n")
	fmt.Fprint(rw, ":\n")
	fmt.Fprint(rw, fmt.Sprintf("retry: %v\n\n", retry))
}
