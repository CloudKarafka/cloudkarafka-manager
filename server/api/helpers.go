package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func internalError(w http.ResponseWriter, bytes interface{}) {
	fmt.Println(bytes)
	w.WriteHeader(http.StatusInternalServerError)
	writeJson(w, bytes)
}

func writeJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bytes)
}
