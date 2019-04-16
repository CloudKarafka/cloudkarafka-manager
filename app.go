package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/db"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/metrics"
	"github.com/cloudkarafka/cloudkarafka-manager/server"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

var (
	port            = flag.String("port", "8080", "Port to run HTTP server on")
	auth            = flag.String("authentication", "scram", "Valid values are (none|none-with-write|scram)")
	retention       = flag.Int("retention", 12, "Retention period (in hours) for historic data, set to 0 to disable history")
	requestTimeout  = flag.Int("request-timeout", 500, "Timeout in ms for requests to brokers to fetch metrics")
	printJMXQueries = flag.Bool("print-jmx-queries", false, "Print all JMX requests to the broker")
	zk              = flag.String("zookeeper", "localhost:2181", "The connection string for the zookeeper connection in the form host:port. Multiple hosts can be given to allow fail-over.")
	kafkaDir        = flag.String("kafkadir", "/opt/kafka", "The directory where kafka lives")
	certsDir        = flag.String("certsdir", "/opt/certs", "The directory where the certificate for kafka is")
)

// TODO: Handle brokers going offline.....
func getBrokerUrls() (map[int]config.HostPort, error) {
	res := make(map[int]config.HostPort)
	brokers, err := zookeeper.Brokers()
	if err != nil {
		return res, err
	}
	for _, id := range brokers {
		broker, err := zookeeper.Broker(id)
		if err != nil {
			return res, err
		}
		res[id] = config.HostPort{broker.Host, broker.Port}
	}
	return res, nil
}

func watchBrokers() {
	data, _, events, _ := zookeeper.WatchChildren("/brokers/ids")
	var (
		current = len(config.BrokerUrls)
		new     = len(data)
		res     = make(map[int]config.HostPort)
	)
	var ids []int
	if new > current {
		for _, id := range data {
			intId, _ := strconv.Atoi(id)
			ids = append(ids, intId)
		}
	} else {
		for id, _ := range config.BrokerUrls {
			ids = append(ids, id)
		}
	}
	le := make(log.MapEntry)
	for _, id := range ids {
		broker, err := zookeeper.Broker(id)
		if err != nil {
			res[id] = config.HostPort{"", -1}
		} else {
			res[id] = config.HostPort{broker.Host, broker.Port}
			le[fmt.Sprintf("%d", id)] = config.HostPort{broker.Host, broker.Port}
		}
	}
	log.Info("broker_change", le)
	config.BrokerUrls = res
	_, ok := <-events
	if ok {
		watchBrokers()
	}
}

func main() {
	flag.Parse()
	signals := make(chan os.Signal, 1)
	quit := make(chan bool, 0)
	signal.Notify(signals, os.Interrupt)

	config.Retention = int64(*retention)
	config.Port = *port
	config.AuthType = *auth
	config.JMXRequestTimeout = time.Duration(*requestTimeout) * time.Millisecond
	config.KafkaDir = *kafkaDir
	config.CertsDir = *certsDir
	config.ZookeeperURL = strings.Split(*zk, ",")
	config.PrintConfig()

	zookeeper.Connect(config.ZookeeperURL)
	zookeeper.SetAuthentication(*auth)

	metrics.TimeRequests = *printJMXQueries
	go watchBrokers()

	if config.Retention > 0 {
		if err := db.Connect(); err != nil {
			log.Error("db_connect", log.MapEntry{"err": err})
			os.Exit(1)
		}
	}
	hourly := time.NewTicker(time.Hour)
	metricsTicker := time.NewTicker(60 * time.Second)
	defer hourly.Stop()
	defer metricsTicker.Stop()
	go func() {
		for {
			select {
			case <-quit:
				return
			case <-metricsTicker.C:
				if config.Retention > 0 {
					metrics.FetchAndStoreMetrics(metrics.TopicBeans, func(v metrics.Metric) string {
						return fmt.Sprintf("topic_metrics/%s/%s/%d", v.Topic, v.Name, v.Broker)
					})
					metrics.FetchAndStoreMetrics(metrics.BrokerBeans, func(v metrics.Metric) string {
						return fmt.Sprintf("broker_metrics/%d/%s", v.Broker, v.Name)
					})
				}
			case <-hourly.C:
				if config.Retention > 0 {
					db.Cleaner(time.Now().Add(time.Hour * time.Duration(config.Retention) * -1))
				}
			}
		}
	}()

	// HTTP server
	go server.Start()
	//Wait for term
	<-signals
	quit <- true
	zookeeper.Stop()
	if config.Retention > 0 {
		db.Close()
	}
}
