#!/bin/bash
set -eux

sudo apt-get update
sudo apt-get install -y build-essential openssl
curl -q -L https://github.com/edenhill/librdkafka/archive/v1.2.2.tar.gz | tar xzf -
pushd librdkafka-1.2.2
./configure --prefix=/usr
make -j
sudo make install
popd
