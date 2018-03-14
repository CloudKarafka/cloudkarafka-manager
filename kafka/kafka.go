package kafka

import (
	"github.com/Shopify/sarama"
	//"github.com/rcrowley/go-metrics"

	"fmt"
	"time"
)

var (
	broker *sarama.Broker
)

func Start(hostname string) error {
	consumeMetrics(hostname)
	return consumerOffsets(hostname)
}

func Stop() {
	if broker != nil {
		broker.Close()
	}
}

func consumePartition(topic string, partition int32, consumer sarama.Consumer, fn func(*sarama.ConsumerMessage)) {
	pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("[ERROR] failed-to-consume topic=%s partition=%v\n", topic, partition)
		fmt.Println(err)
		time.Sleep(30 * time.Second)
		consumePartition(topic, partition, consumer, fn)
		return
	}

	defer func() {
		if err := pc.Close(); err != nil {
			fmt.Println("[ERROR]", err)
		}
	}()

	for {
		select {
		case msg := <-pc.Messages():
			fn(msg)
		case err := <-pc.Errors():
			fmt.Println(err)
		}
	}
}
