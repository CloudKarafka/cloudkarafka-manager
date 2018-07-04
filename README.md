# CloudKarafka Mgmt

## Development

* Clone this repo into $GOPATH/src/cloudkarafka-mgmt
* Run `dep ensure -update`
* Download Kafka
* Download and build cloudkarafka-metrics-reporter
* Copy überjar from cloudkarafka-metrics-reporter to <kafka_dir>/libs
* Add report configs (example below)
* Start kafka `./bin/kafka-server-start config/server.properties`
* Run Management interface with `go run app.go --authentication=none-with-write`

## TODO

* Replace Sarama with confluent-kafka-go
* Always browse topic from beginning?


