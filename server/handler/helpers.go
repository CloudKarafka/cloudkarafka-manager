package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func internalError(w http.ResponseWriter, err error) {
	fmt.Println(err)
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, err.Error())
}

func writeJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bytes)
}
