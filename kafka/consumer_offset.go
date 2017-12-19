package kafka

import (
	"cloudkarafka-mgmt/store"

	"github.com/Shopify/sarama"

	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"
)

type message struct {
	Topic     string
	Type      string
	Group     string
	Partition string
	Offset    int
	Timestamp int64
}

func consumerOffsets(hostname string) error {
	config := sarama.NewConfig()
	h, _ := os.Hostname()
	config.ClientID = fmt.Sprintf("CloudKarafka-co-monitor-%s", h)
	client, err := sarama.NewClient([]string{hostname}, config)
	if err != nil {
		return err
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}
	topic := "__consumer_offsets"

	fmt.Println("[INFO] start consumer offsets consumer")

	partitions, err := client.Partitions(topic)
	if err != nil {
		return err
	}

	for _, part := range partitions {
		go consumePartition(topic, part, consumer)
	}
	return nil
}

func consumePartition(topic string, partition int32, consumer sarama.Consumer) {
	pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("[ERROR] failed-to-consume topic=%s partition=%v\n", topic, partition)
		fmt.Println(err)
		time.Sleep(30 * time.Second)
		consumePartition(topic, partition, consumer)
		return
	}

	defer func() {
		if err := pc.Close(); err != nil {
			fmt.Println("[ERROR]", err)
		}
	}()

	for msg := range pc.Messages() {
		if m, err := processConsumerOffsetsMessage(msg); err == nil {
			if m.Topic == "__consumer_offsets" {
				continue
			}
			store.Put(map[string]string{
				"group":     m.Group,
				"topic":     m.Topic,
				"partition": m.Partition,
			}, m.Offset)
		}
	}
}

func processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) (message, error) {
	var keyver, valver uint16
	var group, topic string
	var partition uint32
	var offset, timestamp uint64

	buf := bytes.NewBuffer(msg.Key)
	err := binary.Read(buf, binary.BigEndian, &keyver)
	var m message
	switch keyver {
	case 0, 1:
		group, err = readString(buf)
		if err != nil {
			return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: group\n", msg.Topic, msg.Partition, msg.Offset))
		}
		topic, err = readString(buf)
		if err != nil {
			return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: topic\n", msg.Topic, msg.Partition, msg.Offset))
		}
		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: partition\n", msg.Topic, msg.Partition, msg.Offset))
		}
	case 2:
		//This is a message when a consumer starts/stop consuming from a topic
		return m, errors.New(fmt.Sprintf("Discarding group metadata message with key version 2\n"))
	default:
		return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: keyver %v\n", msg.Topic, msg.Partition, msg.Offset, keyver))
	}

	buf = bytes.NewBuffer(msg.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if (err != nil) || ((valver != 0) && (valver != 1)) {
		return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: valver %v\n", msg.Topic, msg.Partition, msg.Offset, valver))
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: offset\n", msg.Topic, msg.Partition, msg.Offset))
	}
	_, err = readString(buf)
	if err != nil {
		return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: metadata\n", msg.Topic, msg.Partition, msg.Offset))
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		return m, errors.New(fmt.Sprintf("Failed to decode %s:%v offset %v: timestamp\n", msg.Topic, msg.Partition, msg.Offset))
	}

	m = message{
		Topic:     topic,
		Group:     group,
		Partition: fmt.Sprintf("%v", partition),
		Timestamp: int64(timestamp),
		Offset:    int(offset),
	}
	return m, nil
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen uint16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}
