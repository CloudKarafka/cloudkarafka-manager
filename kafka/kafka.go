package kafka

import (
	"cloudkarafka-mgmt/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"fmt"
	"os"
	"time"
)

func topicMetadata(consumer *kafka.Consumer, topic string) ([]kafka.TopicPartition, error) {
	meta, err := consumer.GetMetadata(&topic, true, 5000)
	if err != nil {
		return nil, err
	}
	if t, ok := meta.Topics[topic]; ok {
		var (
			i      = 0
			toppar = make([]kafka.TopicPartition, len(t.Partitions))
		)
		for _, p := range t.Partitions {
			toppar[i] = kafka.TopicPartition{
				Topic:     &topic,
				Partition: p.ID,
				Offset:    kafka.Offset((time.Now().Unix() - config.Retention) * 1000),
			}
			i++
		}
		return toppar, nil
	}
	return nil, fmt.Errorf("Topic %s doesn't exists", topic)
}

func consumeTopics(consumer *kafka.Consumer, topics []string) error {
	toppar := make([]kafka.TopicPartition, 0)
	i := 0
	topic := topics[i]
	for {
		meta, err := topicMetadata(consumer, topic)
		if err == nil {
			toppar = append(toppar, meta...)
			i = i + 1
			if i > len(topics)-1 {
				break
			}
			topic = topics[i]
		} else {
			fmt.Printf("[ERROR] failed-to-get-partitions topic=%s\n", topic)
			fmt.Println(err)
			time.Sleep(30 * time.Second)
		}
	}
	offsets, err := consumer.OffsetsForTimes(toppar, -1)
	if err != nil {
		return err
	}
	err = consumer.Assign(offsets)
	if err != nil {
		return err
	}
	return nil
}

func Start(hostname string) {
	h, _ := os.Hostname()
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":          hostname,
		"client.id":                  fmt.Sprintf("CloudKarafka-mgmt-%s", h),
		"group.id":                   fmt.Sprintf("CloudKarafka-mgmt-%s", h),
		"queued.max.messages.kbytes": 512,
		"fetch.message.max.bytes":    5120,
		"queued.min.messages":        10,
	}
	topics := []string{"__cloudkarafka_metrics", "__consumer_offsets"}
	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		fmt.Println("[ERROR]", err)
		return
	}
	if err := consumeTopics(consumer, topics); err != nil {
		fmt.Println("[ERROR]", err)
		return
	}
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Println(err)
			continue
		}
		switch *msg.TopicPartition.Topic {
		case "__consumer_offsets":
			consumerOffsetsMessage(msg)
		case "__cloudkarafka_metrics":
			metricMessage(msg)
		}
	}
}
