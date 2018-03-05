package server

import (
	"cloudkarafka-mgmt/store"

	"github.com/gorilla/mux"

	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

func StartJMX() {
	router := mux.NewRouter()
	router.HandleFunc("/kafka.log/Log/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		r.ParseForm()
		switch vars["name"] {
		case "LogStartOffset", "LogEndOffset", "Size":
			value, _ := strconv.Atoi(r.PostForm.Get("Value"))
			data := store.Data{
				Id: map[string]string{
					"metric":    vars["name"],
					"topic":     r.PostForm.Get("topic"),
					"partition": r.PostForm.Get("partition"),
				},
				Value: value,
			}
			store.Put(data, []string{"metric", "topic", "partition"})
		}
	})
	router.HandleFunc("/kafka.server/BrokerTopicMetrics/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		r.ParseForm()
		switch vars["name"] {
		case "BytesInPerSec", "BytesOutPerSec", "MessagesInPerSec":
			topic := r.PostForm.Get("topic")
			value, _ := strconv.ParseFloat(r.PostForm.Get("OneMinuteRate"), 64)
			id := map[string]string{"metric": vars["name"]}
			index := []string{"metric"}
			if topic == "" {
				id["broker"] = r.PostForm.Get("BrokerId")
				index = append(index, "broker")
			} else {
				id["topic"] = topic
				index = append(index, "topic")
			}
			data := store.Data{Id: id, Value: int(value)}
			store.Put(data, index)
		}
	})
	router.HandleFunc("/kafka.server/app-info", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		version := r.PostForm.Get("Version")
		store.KafkaVersion = version
	})
	router.HandleFunc("/{domain}/{type}/{name}", func(w http.ResponseWriter, r *http.Request) {
		//vars := mux.Vars(r)
		//fmt.Printf("domain=%s type=%s name=%s\n", vars["domain"], vars["type"], vars["name"])
	})
	router.HandleFunc("/{domain}/{type}", func(w http.ResponseWriter, r *http.Request) {
		//vars := mux.Vars(r)
		err := r.ParseForm()
		if err != nil {
			fmt.Println("[ERROR]", err)
			body, _ := ioutil.ReadAll(r.Body)
			fmt.Println(string(body))
		} else {
			//fmt.Printf("domain=%s type=%s\n", vars["domain"], vars["type"])
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
