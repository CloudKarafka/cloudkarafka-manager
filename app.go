package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/84codes/cloudkarafka-mgmt/config"
	"github.com/84codes/cloudkarafka-mgmt/db"
	"github.com/84codes/cloudkarafka-mgmt/kafka"
	"github.com/84codes/cloudkarafka-mgmt/server"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"
)

var (
	port      = flag.String("port", "8080", "Port to run HTTP server on")
	kh        = flag.String("kafka", "127.0.0.1:9092", "Hostname and port that the Kafka client should connect to")
	auth      = flag.String("authentication", "scram", "Valid values are (none|none-with-write|scram)")
	retention = flag.Int("retention", 24, "Retention period (in hours) for historic data")
)

func main() {
	flag.Parse()
	signals := make(chan os.Signal, 1)
	quit := make(chan bool, 0)
	signal.Notify(signals, os.Interrupt)

	config.Retention = int64(*retention)
	config.Port = *port
	config.KafkaURL = *kh
	config.AuthType = *auth
	config.PrintConfig()

	err := db.Connect()
	if err != nil {
		fmt.Printf("[ERROR] Could not connect to DB: %s\n", err)
	}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-quit:
				return
			case <-ticker.C:
				db.Cleaner(time.Now().Add(time.Hour * time.Duration(config.Retention) * -1))
			}
		}
	}()

	// Basic info
	zookeeper.Start()
	// Set authentication method for HTTP api
	zookeeper.SetAuthentication(*auth)

	go kafka.Consume(*kh, quit)

	// HTTP server
	go server.Start()
	//Wait for term
	<-signals
	quit <- true
	time.AfterFunc(2*time.Second, func() {
		fmt.Println("[ERROR] could not exit in reasonable time")
		os.Exit(1)
	})
	fmt.Println("[INFO] Stopping mgmt")
	zookeeper.Stop()
	db.Close()
	fmt.Println("[INFO] Stopped successfully")
	return
}
