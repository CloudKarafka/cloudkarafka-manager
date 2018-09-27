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

// Auto populated field during build time
var (
	GitCommit string
	Version   string
)

func main() {
	flag.Parse()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	server.CurrentVersion = server.Version{
		GitCommit, Version,
	}
	fmt.Printf("Build info\n Version:\t%s\n Git commit:\t%s\n", Version, GitCommit)
	fmt.Printf("Runtime\n HTTP Port:\t%s\n Kafka host:\t%s\n Auth typen:\t%s\n Retention:\t%d\n", *port, *kh, *auth, *retention)
	fmt.Println()
	// fmt.Printf("[INFO] authentication-method=%s\n", *auth)

	// Basic info
	zookeeper.Start()
	// Set authentication method for HTTP api
	zookeeper.SetAuthentication(*auth)
	// Runtime metrics, collect metrics every 30s
	// Consumer offsets
	config.Retention = int64(*retention)
	go kafka.Start(*kh)
	// HTTP server
	config.Port = *port
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
