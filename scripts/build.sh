#!/bin/bash

set -eux

export GOOS=linux
export CGO_ENABLED=1
export VERSION=0.3.1

apt-get update
apt-get install -y build-essential
curl -q -L https://github.com/edenhill/librdkafka/archive/v0.11.5.tar.gz | tar xzf -
pushd librdkafka-0.11.5
./configure --prefix=/usr
make -j
make install
popd
mkdir -p /root/bin
curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
dep ensure -update
go build -ldflags "-X github.com/cloudkarafka/cloudkarafka-manager/config.GitCommit=$COMMIT -X github.com/cloudkarafka/cloudkarafka-manager/config.Version=$VERSION" -tags static -a -installsuffix cgo -o cloudkarafka-mgmt.linux
go test -v ./...

