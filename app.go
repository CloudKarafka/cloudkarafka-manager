package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/kafka"
	"github.com/84codes/cloudkarafka-mgmt/server"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

var (
	port      = flag.String("port", "8080", "Port to run HTTP server on")
	kh        = flag.String("kafka", "localhost:9092", "Hostname and port that the Kafka client should connect to")
	auth      = flag.String("authentication", "scram", "Valid values are (none|none-with-write|scram)")
	retention = flag.Int("retention", 10*60, "Retention (in seconds) for in-memory historic data")
)

func main() {
	flag.Parse()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	config.Retention = int64(*retention)
	config.Port = *port
	config.KafkaURL = *kh
	config.AuthType = *auth
	config.PrintConfig()

	// Basic info
	zookeeper.Start()
	// Set authentication method for HTTP api
	zookeeper.SetAuthentication(*auth)

	// Runtime metrics, collect metrics every 30s
	// Consumer offsets
	go kafka.Start(*kh)

	// HTTP server
	go server.Start()
	//Wait for term
	<-signals
	time.AfterFunc(2*time.Second, func() {
		fmt.Println("[ERROR] could not exit in reasonable time")
		os.Exit(1)
	})
	fmt.Println("[INFO] Stopping mgmt")
	zookeeper.Stop()
	kafka.Stop()
	fmt.Println("[INFO] Stopped successfully")
	return
}
