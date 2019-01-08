package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/db"
	"github.com/84codes/cloudkarafka-mgmt/metrics"
	"github.com/84codes/cloudkarafka-mgmt/server"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

var (
	port            = flag.String("port", "8080", "Port to run HTTP server on")
	auth            = flag.String("authentication", "scram", "Valid values are (none|none-with-write|scram)")
	retention       = flag.Int("retention", 24, "Retention period (in hours) for historic data, set to 0 to disable history")
	requestTimeout  = flag.Int("request-timeout", 200, "Timeout in ms for requests to Brokers to fetch metrics")
	printJMXQueries = flag.Bool("print-jmx-queries", false, "Print all JMX requests to the broker")
)

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

func main() {
	flag.Parse()
	signals := make(chan os.Signal, 1)
	quit := make(chan bool, 0)
	signal.Notify(signals, os.Interrupt)

	config.Retention = int64(*retention)
	config.Port = *port
	config.AuthType = *auth
	config.JMXRequestTimeout = time.Duration(*requestTimeout) * time.Millisecond
	config.PrintConfig()

	zookeeper.Start()
	zookeeper.SetAuthentication(*auth)

	metrics.TimeRequests = *printJMXQueries
	brokerUrls, err := getBrokerUrls()
	if err != nil {
		log.Fatalf("[ERROR] Could not get broker urls from Zk: %s\n", err)
		os.Exit(1)
	}
	config.BrokerUrls = brokerUrls
	fmt.Fprintf(os.Stderr, "[INFO] Using brokers: %v\n", brokerUrls)
	if err := db.Connect(); err != nil {
		log.Fatalf("[ERROR] Could not connect to DB: %s\n", err)
		os.Exit(1)
	}
	hourly := time.NewTicker(time.Hour)
	sec30 := time.NewTicker(30 * time.Second)
	defer hourly.Stop()
	defer sec30.Stop()
	go func() {
		for {
			select {
			case <-quit:
				return
			case <-sec30.C:
				if config.Retention > 0 {
					metrics.FetchAndStoreMetrics(metrics.TopicBeans, func(v metrics.Metric) string {
						return fmt.Sprintf("topic_metrics/%s/%s/%d", v.Topic, v.Name, v.Broker)
					})
					metrics.FetchAndStoreMetrics(metrics.BrokerBeans, func(v metrics.Metric) string {
						return fmt.Sprintf("broker_metrics/%d/%s", v.Broker, v.Name)
					})
				}
			case <-hourly.C:
				db.Cleaner(time.Now().Add(time.Hour * time.Duration(config.Retention) * -1))
			}
		}
	}()

	// HTTP server
	go server.Start()
	//Wait for term
	<-signals
	fmt.Println("[INFO] Closing down...")
	quit <- true
	time.AfterFunc(2*time.Second, func() {
		log.Fatal("[ERROR] could not exit in reasonable time")
	})
	zookeeper.Stop()
	db.Close()
	fmt.Println("[INFO] Stopped successfully")
	return
}
