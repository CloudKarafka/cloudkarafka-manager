package main

import (
	"os"
	"os/signal"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/server"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

func main() {
	config.Parse()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	if err := zookeeper.Connect(config.ZookeeperURL); err != nil {
		log.Error("zk_connect", log.ErrorEntry{err})
		os.Exit(1)
		return
	}
	go store.Start()
	go server.Start()
	<-signals
	zookeeper.Stop()
}
