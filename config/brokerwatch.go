package config

import (
	"strconv"

	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

var BrokerChangeListeners = make([]chan map[int]HostPort, 0)

func WatchBrokers() {
	data, _, events, _ := zookeeper.WatchChildren("/brokers/ids")
	res := make(map[int]HostPort)
	for _, id := range data {
		intId, err := strconv.Atoi(id)
		if err != nil {
			continue
		}
		broker, err := zookeeper.Broker(intId)
		if err == nil {
			res[intId] = HostPort{broker.Host, broker.Port}
		}
	}
	for _, l := range BrokerChangeListeners {
		l <- res
	}
	_, ok := <-events
	if ok {
		WatchBrokers()
	}
}
