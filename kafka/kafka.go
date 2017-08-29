package kafka

import (
	"github.com/Shopify/sarama"
	//"github.com/rcrowley/go-metrics"
)

var (
	broker *sarama.Broker
)

func Start() error {
	consumerOffsets()
	return nil
}

func Stop() {
	if broker != nil {
		broker.Close()
	}
}
