package api

import (
	"encoding/json"
	"errors"
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
