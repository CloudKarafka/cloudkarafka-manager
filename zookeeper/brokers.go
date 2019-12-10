package zookeeper

import (
	"fmt"
	"strconv"
)

type B struct {
	Version   int      `json:"-"`
	JmxPort   int      `json:"jmx_port"`
	Timestamp string   `json:"timestamp"`
	Endpoints []string `json:"endpoints"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Id        int      `json:"id"`
}

// Controller struct from Zookeeper Path "/controller"
type C struct {
	Timestamp string `json:"timestamp"`
	BrokerId  int    `json:"brokerid"`
	Version   int    `json:"version"`
}

func Brokers() ([]int, error) {
	stringIds, err := all("/brokers/ids", func(string) bool { return true })
	if err != nil {
		return nil, err
	}
	ids := make([]int, len(stringIds))
	for i, id := range stringIds {
		if intId, err := strconv.Atoi(id); err == nil {
			ids[i] = intId
		}
	}
	return ids, nil
}

func Broker(id int) (B, error) {
	var b B
	err := get(fmt.Sprintf("/brokers/ids/%d", id), &b)
	b.Id = id
	return b, err
}

func Controller() (C, error) {
	var c C
	err := get("/controller", &c)
	return c, err
}

type HostPort struct {
	Id   int
	Host string
	Port int
}

var brokersListeners = make([]chan []HostPort, 0, 10)

func WatchBrokers() chan []HostPort {
	ch := make(chan []HostPort)
	brokersListeners = append(brokersListeners, ch)
	return ch
}

func watchBrokers() {
	data, _, events, _ := WatchChildren("/brokers/ids")
	list := make([]HostPort, len(data))
	for i, id := range data {
		intId, err := strconv.Atoi(id)
		if err != nil {
			continue
		}
		broker, err := Broker(intId)
		if err != nil {
			continue
		}
		list[i] = HostPort{intId, broker.Host, broker.Port}
	}
	fanoutCH <- list
	_, ok := <-events
	if ok {
		watchBrokers()
	}
}
