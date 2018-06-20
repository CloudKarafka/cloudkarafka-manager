package kafka

import (
	"github.com/Shopify/sarama"

	"fmt"
	"os"
	"sync"
	"time"
)

var (
	c conn
)

type conn struct {
	client    sarama.Client
	consumers []sarama.Consumer
	l         sync.Mutex
}

func (me conn) ConsumeTopic(topic string, handleMessage func(*sarama.ConsumerMessage)) {
	consumer, err := sarama.NewConsumerFromClient(me.client)
	if err != nil {
		fmt.Println("[ERROR] failed-to-create-consumer retrying")
		fmt.Println(err)
		time.Sleep(30 * time.Second)
		me.ConsumeTopic(topic, handleMessage)
		return
	}
	me.l.Lock()
	me.consumers = append(me.consumers, consumer)
	me.l.Unlock()
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("[ERROR] failed-to-get-partitions topic=%s\n", topic)
		fmt.Println(err)
		time.Sleep(30 * time.Second)
		me.ConsumeTopic(topic, handleMessage)
		return
	}
	for _, part := range partitions {
		go me.consumePartition(topic, part, consumer, handleMessage)
	}
}

func (me conn) consumePartition(topic string, partition int32, consumer sarama.Consumer, fn func(*sarama.ConsumerMessage)) {
	ts := time.Now().Add(-5 * time.Minute).UTC().Unix()
	offset, _ := me.client.GetOffset(topic, partition, ts*1000)
	pc, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		fmt.Printf("[ERROR] failed-to-consume topic=%s partition=%v\n", topic, partition)
		fmt.Println(err)
		time.Sleep(30 * time.Second)
		me.consumePartition(topic, partition, consumer, fn)
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

func (me conn) TopicExists(name string) bool {
	topics, err := me.client.Topics()
	if err != nil {
		fmt.Println(err)
		return false
	}
	exists := false
	for _, t := range topics {
		if t == name {
			exists = true
			break
		}
	}
	return exists
}

func (me conn) Stop() {
	me.l.Lock()
	for _, c := range me.consumers {
		c.Close()
	}
	me.l.Unlock()
	me.client.Close()
}

func Start(hostname string) {
	config := sarama.NewConfig()
	h, _ := os.Hostname()
	config.ClientID = fmt.Sprintf("CloudKarafka-mgmt-%s", h)
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_0
	client, err := sarama.NewClient([]string{hostname}, config)
	if err != nil {
		fmt.Println("[ERROR]", err)
		return
	}
	c = conn{client: client}
	if c.TopicExists("__consumer_offsets") {
		go c.ConsumeTopic("__consumer_offsets", consumerOffsetsMessage)
	}
	if c.TopicExists("__cloudkarafka_metrics") {
		go c.ConsumeTopic("__cloudkarafka_metrics", metricMessage)
	}
}

func Stop() {
	c.Stop()
}
