# CloudKarafka Mgmt

 [![Run Status](https://api.shippable.com/projects/5bab6ccbe8c96c070042aa6c/badge?branch=master)]() 
 
## Development

* Clone this repo into $GOPATH/src/cloudkarafka-mgmt
* Run `dep ensure -update`
* Download Kafka
* Download and build cloudkarafka-metrics-reporter
* Copy überjar from cloudkarafka-metrics-reporter to <kafka_dir>/libs
* Add report configs (example below)
* Start kafka `./bin/kafka-server-start config/server.properties`
* Run Management interface with `go run app.go --authentication=none-with-write`



