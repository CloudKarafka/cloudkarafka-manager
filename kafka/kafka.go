package kafka

import (
	"cloudkarafka-mgmt/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"fmt"
	"os"
	"time"
)

var (
	c conn
)

type conn struct {
	consumer *kafka.Consumer
}

func (me conn) ConsumeTopic(topic string) {
	meta, err := me.consumer.GetMetadata(&topic, true, 5000)
	if err != nil {
		fmt.Printf("[ERROR] failed-to-get-partitions topic=%s\n", topic)
		fmt.Println(err)
		time.Sleep(30 * time.Second)
		me.ConsumeTopic(topic)
		return
	}
	if t, ok := meta.Topics[topic]; ok {
		var (
			i     = 0
			times = make([]kafka.TopicPartition, len(t.Partitions))
		)
		for _, p := range t.Partitions {
			times[i] = kafka.TopicPartition{
				Topic:     &topic,
				Partition: p.ID,
				Offset:    kafka.Offset((time.Now().Unix() - config.Retention) * 1000),
			}
			i++
		}
		offsets, err := me.consumer.OffsetsForTimes(times, 10000)
		if err != nil {
			fmt.Printf("[ERROR] failed to get offsets for topic (%s)\n", topic)
			fmt.Println(err)
			return
		}
		err = me.consumer.Assign(offsets)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func (me conn) TopicExists(name string) bool {
	meta, err := me.consumer.GetMetadata(&name, true, 5000)
	if err != nil {
		fmt.Println(err)
		return false
	}
	_, ok := meta.Topics[name]
	return ok
}

func (me conn) Stop() {
	me.consumer.Close()
}

func Start(hostname string) {
	h, _ := os.Hostname()
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":          hostname,
		"client.id":                  fmt.Sprintf("CloudKarafka-mgmt-%s", h),
		"group.id":                   fmt.Sprintf("CloudKarafka-mgmt-%s", h),
		"queued.max.messages.kbytes": 1024,
		"fetch.max.bytes":            1048576,
		"queued.min.messages":        1000,
	}
	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		fmt.Println("[ERROR]", err)
		return
	}
	c = conn{consumer: consumer}
	if c.TopicExists("__consumer_offsets") {
		c.ConsumeTopic("__consumer_offsets")
	}
	if c.TopicExists("__cloudkarafka_metrics") {
		c.ConsumeTopic("__cloudkarafka_metrics")
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

func Stop() {
	c.Stop()
}
