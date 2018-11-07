package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/db"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	consumer *kafka.Consumer
)

func topicMetadata(consumer *kafka.Consumer, topic string) ([]kafka.TopicPartition, error) {
	meta, err := consumer.GetMetadata(&topic, false, 5000)
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
				Offset:    kafka.OffsetEnd, //kafka.Offset((time.Now().Unix() - config.Retention) * 1000),
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
			fmt.Printf("[WARN] failed-to-get-partitions topic=%s, err=%s\n", topic, err)
			time.Sleep(30 * time.Second)
		}
	}
	//offsets, err := consumer.OffsetsForTimes(toppar, -1)
	//if err != nil {
	//return err
	//}
	return consumer.Assign(toppar)
}

func Float64frombytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func Consume(hostname string, quit chan bool) {
	h, _ := os.Hostname()
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        hostname,
		"go.events.channel.enable": true,
		"client.id":                h,
		"group.id":                 "CloudKarafka-mgmt",
		//"fetch.wait.max.ms":        5000,
		//"fetch.min.bytes":          1000,
		//"queued.max.messages.kbytes": 512,
		//"fetch.message.max.bytes":    5120,
		//"queued.min.messages":        10,
	}
	topics := []string{"__cloudkarafka_metrics"}
	var err error
	consumer, err = kafka.NewConsumer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not create kafka consumer: %s\n", err)
		return
	}
	if err := consumeTopics(consumer, topics); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Kafka: assign partitions failed: %s\n", err)
		return
	}
	events := consumer.Events()
	defer func() {
		fmt.Fprintf(os.Stderr, "[INFO] Closing kafka consumer\n")
		consumer.Close()
	}()
	// Around 200 msg per batch is retrived and written to DB
	// So this is a good start size of the slice, will allocate more if needed
	tmp := make([]db.DBValue, 200)
	i := 0
	batchCounter := 1
	for {
		select {
		case e := <-events:
			switch ev := e.(type) {
			case *kafka.Message:
				var value MetricMessage
				if err := json.Unmarshal(ev.Value, &value); err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] Couldn't parse %s, error: %s\n", string(ev.Value), err)
				} else {
					value.Timestamp = ev.Timestamp
					if i >= len(tmp) {
						tmp = append(tmp, make([]db.DBValue, 10)...)
					}
					tmp[i] = value
					i += 1
				}
			case kafka.OffsetsCommitted:
				fmt.Fprintf(os.Stderr, "[INFO] Kafka: Got metrics batch #%d from collector\n", batchCounter)
				// A small hack to remove stale consumer groups
				// Offset is commited roughly every 30 seconds, to %120 is every hour
				if batchCounter%120 == 0 {
					db.DeleteBucket([]byte("groups"))
				}
				batchCounter += 1
				db.Write(tmp[:i])
				i = 0
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "[ERROR] Kafka error: %s\n", ev)
			}
		case <-quit:
			return
		}
	}
}
