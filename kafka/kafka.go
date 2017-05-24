package kafka

import (
	"github.com/Shopify/sarama"

	"time"
)

var (
	broker *sarama.Broker
)

func Start() error {
	//client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	//if err != nil {
	//return err
	//}
	//consumer, err := sarama.NewConsumerFromClient(client)
	//if err != nil {
	//return err
	//}
	//go consumerOffsets(s)
	connect()
	return nil
}

func Topics() ([]*sarama.TopicMetadata, error) {
	resp, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		connect()
		return []*sarama.TopicMetadata{}, err
	}
	return resp.Topics, nil
}

func connect() {
	if broker != nil {
		broker.Close()
	}
	url := "localhost:9092"
	config := sarama.NewConfig()
	config.ClientID = "CloudKarafka-consumer-offset-monitor"
	broker = sarama.NewBroker(url)
	if err := broker.Open(config); err != nil {
		time.Sleep(1 * time.Second)
		connect()
	}
}

func Stop() {
	if broker != nil {
		broker.Close()
	}
}
