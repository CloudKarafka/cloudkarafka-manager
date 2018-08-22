package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

var client = http.Client{}

func internalError(w http.ResponseWriter, bytes interface{}) {
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Println(bytes)
	str, ok := bytes.(string)
	if ok {
		fmt.Fprintf(w, str)
		return
	}
	WriteJson(w, bytes)
}

func WriteJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bytes)
}

func fetchRemote(path string, r *http.Request, out interface{}) error {
	req, err := http.NewRequest("get", path, nil)
	u, p, _ := r.BasicAuth()
	req.SetBasicAuth(u, p)
	res, err := client.Do(req)
	if err != nil {
		return err
	} else {
		dec := json.NewDecoder(res.Body)
		defer res.Body.Close()
		return dec.Decode(&out)
	}
}
