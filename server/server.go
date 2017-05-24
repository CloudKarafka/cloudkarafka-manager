package server

import (
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/zookeeper"

	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"
)

type base struct {
	Brokers []broker
	Topics  []topic
}

type broker struct {
	Id, Version     string
	AdvertisedPorts interface{}
	Uptime          time.Duration
}

type topic struct {
	Name, Replicas, Config string
	Partitions             int
}

func generateViewModel() base {
	var (
		brokers []broker
		topics  []topic
	)

	for _, b := range zookeeper.Brokers() {
		fmt.Println(b)
		if id, ok := b["id"].(string); ok {
			ts, _ := strconv.ParseInt(b["timestamp"].(string), 10, 64)
			t := time.Unix(ts/1000, 0)
			uptime := time.Since(t)
			endpoints := b["endpoints"]
			brokers = append(brokers, broker{
				Id:              id,
				Uptime:          uptime,
				AdvertisedPorts: endpoints,
			})
		}
	}
	ts, err := kafka.Topics()
	if err == nil {
		for _, t := range ts {
			topics = append(topics, topic{
				Name:       t.Name,
				Partitions: len(t.Partitions),
			})
		}
	}
	return base{Brokers: brokers, Topics: topics}
}

func home(w http.ResponseWriter, r *http.Request) {
	t, _ := template.ParseFiles("server/views/home.tpl")
	vm := generateViewModel()
	t.Execute(w, vm)
}

func Start(port string) {
	http.HandleFunc("/", home)
	s := &http.Server{
		Addr:         port,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	s.ListenAndServe()
}
