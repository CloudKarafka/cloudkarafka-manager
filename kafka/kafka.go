package kafka

import (
	"github.com/Shopify/sarama"

	"fmt"
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

func Consumers() ([]*sarama.GroupDescription, error) {
	resp, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{})
	fmt.Println(err)
	if err != nil {
		connect()
		return make([]*sarama.GroupDescription, 0), err
	}
	return resp.Groups, nil
}

func Consumer(name string) (*sarama.GroupDescription, error) {
	resp, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{
		Groups: []string{name},
	})
	if err != nil {
		connect()
		return nil, err
	}
	return resp.Groups[0], nil
}

func Topic(name string) (*sarama.TopicMetadata, error) {
	resp, err := broker.GetMetadata(&sarama.MetadataRequest{Topics: []string{name}})
	return resp.Topics[0], err
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
