#!/bin/bash -ex

trap '[ $? -ne 0 ]; rm -rf target' EXIT

mkdir -p target

GOARCH=amd64 GOOS=linux go build -o target/cloudkarafka-mgmt.linux
go build -o target/cloudkarafka-mgmt.amd64.osx
cp -r jars target/jars
cp -r static target/static
tar -czf cloudkarafka-mgmt.tar.gz target
rm -rf target
aws s3 cp cloudkarafka-mgmt.tar.gz s3://cloudkafka-manager/cloudkarafka-mgmt.tar.gz
rm -rf coudkarafka-mgmt.tar.gz
