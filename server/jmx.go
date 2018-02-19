package server

import (
	"github.com/gorilla/mux"

	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

func StartJMX() {
	router := mux.NewRouter()
	router.HandleFunc("/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		err := r.ParseForm()
		if err != nil {
			fmt.Println("[ERROR]", err)
			body, _ := ioutil.ReadAll(r.Body)
			fmt.Println(string(body))
		} else {
			val := r.PostForm.Get("value")
			tag := r.PostForm.Get("tag")
			fmt.Printf("metric-name=%s value=%s tag=%s\n", vars["name"], val, tag)
		}
	})
	s := &http.Server{
		Handler:      router,
		Addr:         "localhost:7070",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	fmt.Println("Listening on Port 7070")
	fmt.Println(s.ListenAndServe())
}
