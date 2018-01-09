package main

import (
	"cloudkarafka-mgmt/config"
	"cloudkarafka-mgmt/jmx"
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/server"
	"cloudkarafka-mgmt/zookeeper"

	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
)

var (
	port = flag.String("port", "8080", "Port to run HTTP server on")
	kh   = flag.String("kafka", "localhost:9092", "Hostname and port that the Kafka client should connect to")
	key  = flag.String("key", "", "Path to CA key")
	cert = flag.String("cert", "", "Path to CA cert")
	auth = flag.String("authentication", "none-with-write", "Valid values are (none|none-with-write|scram)")
)

func main() {
	flag.Parse()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Basic info
	zookeeper.Start()
	// Set authentication method for HTTP api
	fmt.Printf("[INFO] authentication-method=%s\n", *auth)
	zookeeper.SetAuthentication(*auth)
	// Runtime metrics, collect metrics every 30s
	go jmx.Start(30)
	// Consumer offsets
	if err := kafka.Start(*kh); err != nil {
		fmt.Println("[WARN] Kakfa client failed to start no consumer info i shown")
		fmt.Println(err)
	}
	// HTTP server
	config.Port = *port
	go server.Start(*cert, *key)
	fmt.Println("CloudKarafka mgmt interface for Apache Kafka started")
	//Wait for term
	<-signals
	time.AfterFunc(2*time.Second, func() {
		fmt.Println("[ERROR] could not exit in reasonable time")
		os.Exit(1)
	})
	kafka.Stop()
	zookeeper.Stop()
	jmx.Exit()
	fmt.Println("Stopped successfully")
}
