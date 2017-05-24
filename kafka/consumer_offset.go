package kafka

/*
import (
	"github.com/Shopify/sarama"

	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

type message struct {
	cluster   string
	Metric    string
	Topic     string
	Type      string
	Group     string
	Partition int
	Offset    int
	Timestamp int64
}

func (me message) ClusterName() string {
	return me.cluster
}

func consumerOffsets(s *shovel) error {
	topic := "__consumer_offsets"

	if s.Started {
		return nil
	}
	s.Started = true
	fmt.Printf("[INFO] start shovels for %s\n", s.cluster)

	partitions, err := s.client.Partitions(topic)
	if err != nil {
		return err
	}

	for _, part := range partitions {
		go s.consumePartition(topic, part)
	}
	return nil
}

func (me *shovel) consumePartition(topic string, partition int32) {
	pc, err := me.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("[ERROR] failed-to-consume cluster=%s topic=%s partition=%v\n", me.cluster, topic, partition)
		fmt.Println(err)
		time.Sleep(30 * time.Second)
		me.consumePartition(topic, partition)
		return
	}

	defer func() {
		if err := pc.Close(); err != nil {
			fmt.Println("[ERROR]", err)
		}
	}()

	for msg := range pc.Messages() {
		if m, err := me.processConsumerOffsetsMessage(msg); err == nil {
			if m.Topic == "__consumer_offsets" {
				continue
			}
		}
	}
}

func (me *shovel) processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) (message, error) {
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
		Metric:    "consumer-offsets",
		Type:      "kafka",
		cluster:   me.cluster,
		Topic:     topic,
		Group:     group,
		Partition: int(partition),
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
*/
