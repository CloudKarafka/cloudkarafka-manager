#!/bin/bash

set -eux

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
