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
	Timeout    time.Duration = 10 * time.Second
	SampleTime time.Duration = 2 * time.Second
)

func handleBrokerChanges(hps []zookeeper.HostPort) []MetricRequest {
	reqs := make([]MetricRequest, len(hps)*4)
	for i, hp := range hps {
		copy(reqs[i*4:], []MetricRequest{
			MetricRequest{hp.Id, BeanBrokerBytesInPerSec},
			MetricRequest{hp.Id, BeanBrokerBytesOutPerSec},
			MetricRequest{hp.Id, BeanBrokerIsrExpands},
			MetricRequest{hp.Id, BeanBrokerIsrShrinks},
		})
		broker, _ := fetchBroker(hp.Id)
		broker.KafkaVersion = "2.5.0" // TODO
		DB.UpdateBroker(broker)
	}
	return reqs
}

func updateVersionRequests(hps []zookeeper.HostPort) []VersionRequest {
	reqs := make([]VersionRequest, len(hps))
	for i, hp := range hps {
		reqs[i] = VersionRequest{hp.Id, "kafka"}
	}
	return reqs
}

func handleTopicChanges(topics []zookeeper.T) []MetricRequest {
	reqs := make([]MetricRequest, 0, 5*len(topics))
	for _, t := range topics {
		topic, _ := FetchTopic(t.Name)
		DB.UpdateTopic(topic)
		if strings.HasPrefix(topic.Name, "__") {
			continue
		}
		if 200 < len(topic.Partitions) {
			log.Info("fetch_metrics", log.StringEntry(
				fmt.Sprintf("Topic %s has more than 200 partitions, skipping jmx metrics", topic.Name)))
			continue
		}
		for i, p := range topic.Partitions {
			if p.Leader != -1 {
				reqs = append(reqs, []MetricRequest{
					MetricRequest{p.Leader, BeanTopicLogSize(t.Name, i)},
					MetricRequest{p.Leader, BeanTopicLogEnd(t.Name, i)},
					MetricRequest{p.Leader, BeanTopicLogStart(t.Name, i)},
					MetricRequest{p.Leader, BeanTopicBytesOutPerSec(t.Name)},
					MetricRequest{p.Leader, BeanTopicBytesInPerSec(t.Name)},
				}...)
			}
		}
	}
	return reqs
}

func Start() {
	var (
		brokerRequests  []MetricRequest
		topicRequests   []MetricRequest
		versionRequests []VersionRequest
		cancel          context.CancelFunc

		topicChanges  = make(chan []zookeeper.T)
		brokerChanges = make(chan []zookeeper.HostPort)
		cMetrics      = make(chan ConsumerGroup)
		tMetrics      = make(chan Metric)
		bMetrics      = make(chan Metric)
		vMetrics      = make(chan Version)
		ec            = make(chan error)
		ticker        = time.NewTicker(SampleTime)

		topicMetricPool   = &JMXBroker{conns: make(map[int]JMXConn)}
		brokerMetricPool  = &JMXBroker{conns: make(map[int]JMXConn)}
		groupMetricPool   = &JMXBroker{conns: make(map[int]JMXConn)}
		versionMetricPool = &JMXBroker{conns: make(map[int]JMXConn)}
	)
	zookeeper.WatchTopics(topicChanges)
	zookeeper.WatchBrokers(brokerChanges)

	for {
		select {
		case <-ticker.C:
			if cancel != nil {
				cancel()
			}
			go func(reqs []MetricRequest) {
				for _, req := range reqs {
					if e := GetMetrics(topicMetricPool, req, tMetrics); e != nil {
						ec <- e
					}
				}
			}(topicRequests)
			go func(reqs []MetricRequest) {
				for _, req := range reqs {
					if e := GetMetrics(brokerMetricPool, req, bMetrics); e != nil {
						ec <- e
					}
				}
			}(brokerRequests)

			go func(reqs []VersionRequest) {
				for _, req := range reqs {
					if e := GetVersion(versionMetricPool, req, vMetrics); e != nil {
						ec <- e
					}
				}
			}(versionRequests)
			go func() {
				if e := GetGroups(groupMetricPool, cMetrics); e != nil {
					ec <- e
				}
			}()
		case hps := <-brokerChanges:
			brokerRequests = handleBrokerChanges(hps)
			versionRequests = updateVersionRequests(hps)
			topicMetricPool.CloseAll()
			brokerMetricPool.CloseAll()
			groupMetricPool.CloseAll()
			versionMetricPool.CloseAll()
			for _, hp := range hps {
				uri := fmt.Sprintf("%s:2%d", hp.Host, hp.Port)
				if err := topicMetricPool.Connect(hp.Id, uri); err != nil {
					ec <- err
				}
				if err := brokerMetricPool.Connect(hp.Id, uri); err != nil {
					ec <- err
				}
				if err := versionMetricPool.Connect(hp.Id, uri); err != nil {
					ec <- err
				}
				if err := groupMetricPool.Connect(hp.Id, uri); err != nil {
					ec <- err
				}
			}
		case topics := <-topicChanges:
			topicRequests = handleTopicChanges(topics)
		case metric := <-bMetrics:
			DB.UpdateBrokerMetrics(metric)
		case metric := <-tMetrics:
			DB.UpdateTopicMetric(metric)
		case v := <-vMetrics:
			DB.UpdateBrokerVersion(v)
		case cg := <-cMetrics:
			DB.UpdateConsumer(cg)
		case e := <-ec:
			log.Error("jmx", log.ErrorEntry{e})
		}
	}
}

func adminClient() (*kafka.AdminClient, error) {
	adminConfig := &kafka.ConfigMap{"bootstrap.servers": strings.Join(config.BrokerUrls.List(), ",")}
	return kafka.NewAdminClient(adminConfig)
}
