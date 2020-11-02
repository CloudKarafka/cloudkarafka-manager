package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	MaxPoints  int           = 500
	Timeout    time.Duration = 5 * time.Second
	SampleTime time.Duration = 10 * time.Second
)

func FetchMetrics(ctx context.Context, metrics chan Metric, reqs []MetricRequest) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	for _, r := range reqs {
		select {
		case <-ctx.Done():
			log.Error("fetch_metrics", log.ErrorEntry{ctx.Err()})
			break
		default:
			fmt.Println("metrics req", r)
			resp, err := GetMetrics(ctx, r)
			if err != nil {
				log.Error("fetch_metrics", log.ErrorEntry{err})
				continue
			}
			for _, r := range resp {
				metrics <- r
			}
		}
	}
}

func handleBrokerChanges(hps []zookeeper.HostPort) []MetricRequest {
	for _, hp := range hps {
		broker, _ := fetchBroker(hp.Id)
		store.UpdateBroker(broker)
	}
	return nil
}

func handleTopicChanges(topics []zookeeper.T) []MetricRequest {
	for _, t := range topics {
		topic, _ := FetchTopic(t.Name)
		store.UpdateTopic(topic)
	}
	return nil
}

func Start() {
	var (
		ctx    context.Context
		cancel context.CancelFunc

		topicChanges  = make(chan []zookeeper.T)
		brokerChanges = make(chan []zookeeper.HostPort)
		bMetrics      = make(chan Metric)
		tMetrics      = make(chan Metric)
		cMetrics      = make(chan ConsumerGroups)
		ticker        = time.NewTicker(SampleTime)
	)

	zookeeper.WatchTopics(topicChanges)
	zookeeper.WatchBrokers(brokerChanges)

	defer ticker.Stop()
	defer close(bMetrics)
	defer close(tMetrics)
	defer close(topicChanges)
	defer close(brokerChanges)
	for {
		select {
		case <-ticker.C:
			if cancel != nil {
				cancel()
			}
			ctx, cancel = context.WithCancel(context.Background())
			go FetchConsumerGroups(ctx, cMetrics)
		case hps := <-brokerChanges:
			handleBrokerChanges(hps)
		case topics := <-topicChanges:
			handleTopicChanges(topics)
		case metric := <-bMetrics:
			store.UpdateBrokerMetrics(metric)
		case metric := <-tMetrics:
			store.UpdateTopicMetric(metric)
		case cgs := <-cMetrics:
			store.UpdateConsumers(cgs)
		}
	}
}

func adminClient() (*kafka.AdminClient, error) {
	adminConfig := &kafka.ConfigMap{"bootstrap.servers": strings.Join(config.BrokerUrls.List(), ",")}
	return kafka.NewAdminClient(adminConfig)
}
