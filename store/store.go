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

type MetricsFetcher struct {
	Conn JMXConn
	Out  chan Metric
}

func (me MetricsFetcher) Stop() {
	close(me.Out)
	me.Conn.Close()
}

func (me MetricsFetcher) Run(ctx context.Context, reqs []MetricRequest) {
	for _, r := range reqs {
		resp, err := me.Conn.GetMetrics(r)
		if err != nil {
			log.Error("fetch_metrics", log.ErrorEntry{err})
			continue
		}
		for _, r := range resp {
			me.Out <- r
		}
	}
}

func handleBrokerChanges(hps []zookeeper.HostPort) []MetricRequest {
	reqs := make([]MetricRequest, len(hps)*4)
	for i, hp := range hps {
		copy(reqs[i*4:], []MetricRequest{
			MetricRequest{hp.Id, BeanBrokerBytesInPerSec, "Count"},
			MetricRequest{hp.Id, BeanBrokerBytesOutPerSec, "Count"},
			MetricRequest{hp.Id, BeanBrokerIsrExpands, "Count"},
			MetricRequest{hp.Id, BeanBrokerIsrShrinks, "Count"},
		})
		broker, _ := fetchBroker(hp.Id)
		store.UpdateBroker(broker)
	}
	return reqs
}

func handleTopicChanges(topics []zookeeper.T) []MetricRequest {
	reqs := make([]MetricRequest, 0, 5*len(topics))
	for _, t := range topics {
		topic, _ := FetchTopic(t.Name)
		store.UpdateTopic(topic)
		if 200 < len(topic.Partitions) {
			continue
		}
		for _, p := range topic.Partitions {
			if p.Leader != -1 {
				reqs = append(reqs, []MetricRequest{
					MetricRequest{p.Leader, BeanTopicLogSize(t.Name), "Value"},
					MetricRequest{p.Leader, BeanTopicLogEnd(t.Name), "Value"},
					MetricRequest{p.Leader, BeanTopicLogStart(t.Name), "Value"},
					MetricRequest{p.Leader, BeanTopicBytesOutPerSec(t.Name), "Count"},
					MetricRequest{p.Leader, BeanTopicBytesInPerSec(t.Name), "Count"},
				}...)
			}
		}
	}
	return reqs
}

func Start() {
	var (
		brokerRequests []MetricRequest
		topicRequests  []MetricRequest
		ctx            context.Context
		cancel         context.CancelFunc

		topicChanges  = make(chan []zookeeper.T)
		brokerChanges = make(chan []zookeeper.HostPort)
		cMetrics      = make(chan ConsumerGroups)
		ticker        = time.NewTicker(SampleTime)
	)
	defer ticker.Stop()
	defer close(topicChanges)
	defer close(brokerChanges)

	conn, err := DialJMXServer()
	if err != nil {
		fmt.Println(err)
		return
	}
	bMetrics := MetricsFetcher{conn, make(chan Metric)}
	defer bMetrics.Stop()

	conn, err = DialJMXServer()
	if err != nil {
		fmt.Println(err)
		return
	}
	tMetrics := MetricsFetcher{conn, make(chan Metric)}
	defer tMetrics.Stop()

	zookeeper.WatchTopics(topicChanges)
	zookeeper.WatchBrokers(brokerChanges)
	fmt.Println(brokerRequests)
	for {
		select {
		case <-ticker.C:
			if cancel != nil {
				cancel()
			}
			ctx, cancel = context.WithCancel(context.Background())
			go bMetrics.Run(ctx, brokerRequests)
			go tMetrics.Run(ctx, topicRequests)
			go FetchConsumerGroups(ctx, cMetrics)
		case hps := <-brokerChanges:
			brokerRequests = handleBrokerChanges(hps)
		case topics := <-topicChanges:
			topicRequests = handleTopicChanges(topics)
		case metric := <-bMetrics.Out:
			store.UpdateBrokerMetrics(metric)
		case metric := <-tMetrics.Out:
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
