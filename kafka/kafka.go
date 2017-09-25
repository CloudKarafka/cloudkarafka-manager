package kafka

import (
	"github.com/Shopify/sarama"
	//"github.com/rcrowley/go-metrics"
)

var (
	broker *sarama.Broker
)

func Start(hostname string) error {
	return consumerOffsets(hostname)
}

func Stop() {
	if broker != nil {
		broker.Close()
	}
}
