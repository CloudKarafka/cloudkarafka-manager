# CloudKarafka Manager

## Usage

* Download the latest version from the releases and extract the file
* Make sure all your brokers have the [Kafka HTTP Reporter](https://github.com/CloudKarafka/KafkaHttpReporter) installed
* Start the application: `./cloudkarafka-mgmt.linux`
* Open your web browser and go to [http://localhost:8080](http://localhost:8080)

## Development

* Clone this repo into $GOPATH/src/github.com/CloudKarafka/cloudkarafka-manager
* Run `dep ensure -update`
* Install the metrics reporter [Kafka HTTP Reporter](https://github.com/CloudKarafka/KafkaHttpReporter) on your local kafka broker
* Run Management interface with `go run app.go --authentication=none-with-write`

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/CloudKarafka/KafkaHttpReporter/tags). 

## Authors

* **Magnus Hörberg** - *Initial work* - [magnushoerberg](https://github.com/magnushoerberg)
* **Magnus Landerblom** - *Initial work* - [snichme](https://github.com/snichme)


