package main

import (
	"cloudkarafka-mgmt/config"
	"cloudkarafka-mgmt/kafka"
	"cloudkarafka-mgmt/server"
	"cloudkarafka-mgmt/zookeeper"

	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"time"
)

var (
	port      = flag.String("port", "8080", "Port to run HTTP server on")
	kh        = flag.String("kafka", "localhost:9092", "Hostname and port that the Kafka client should connect to")
	auth      = flag.String("authentication", "scram", "Valid values are (none|none-with-write|scram)")
	retention = flag.Int("retention", 5*60, "Retention (in seconds) for in-memory historic data")
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
	// Consumer offsets
	config.Retention = int64(*retention)
	go kafka.Start(*kh)
	// HTTP server
	config.Port = *port
	// 5 minutes in seconds
	go server.Start()
	fmt.Println("CloudKarafka mgmt interface for Apache Kafka started")
	printMemUsage()
	//Wait for term
loop:
	for {
		select {
		case <-signals:
			time.AfterFunc(2*time.Second, func() {
				fmt.Println("[ERROR] could not exit in reasonable time")
				os.Exit(1)
			})
			break loop
		case <-time.After(10 * time.Second):
			printMemUsage()
		}
	}
	fmt.Println("Stopping mgmt")
	zookeeper.Stop()
	fmt.Println("Stopped successfully")
	return
}

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc %v MiB\t", m.Alloc/1024/1024)
	fmt.Printf("TotalAlloc %v MiB\t", m.TotalAlloc/1024/1024)
	fmt.Printf("Sys %v MiB\t", m.Sys/1024/1024)
	fmt.Printf("NumGC %v\n", m.NumGC)
}
