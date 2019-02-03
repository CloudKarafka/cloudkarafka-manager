package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	retention       = flag.Int("retention", 12, "Retention period (in hours) for historic data, set to 0 to disable history")
	requestTimeout  = flag.Int("request-timeout", 500, "Timeout in ms for requests to brokers to fetch metrics")
	printJMXQueries = flag.Bool("print-jmx-queries", false, "Print all JMX requests to the broker")
	zk              = flag.String("zookeeper", "localhost:2181", "The connection string for the zookeeper connection in the form host:port. Multiple hosts can be given to allow fail-over.")
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
	fmt.Fprintf(os.Stderr, "[INFO] Number of brokers changed: previous=%d, now=%d\n", current, new)
	for _, id := range ids {
		broker, err := zookeeper.Broker(id)
		if err != nil {
			res[id] = config.HostPort{"", -1}
		} else {
			res[id] = config.HostPort{broker.Host, broker.Port}
		}
	}
	fmt.Fprintf(os.Stderr, "[INFO] Using brokers: %v\n", res)
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
	config.PrintConfig()

	zookeeper.Connect(strings.Split(*zk, ","))
	zookeeper.SetAuthentication(*auth)

	metrics.TimeRequests = *printJMXQueries
	go watchBrokers()

	if err := db.Connect(); err != nil {
		log.Fatalf("[ERROR] Could not connect to DB: %s\n", err)
		os.Exit(1)
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
