#!/bin/bash -ex

trap '[ $? -ne 0 ]; rm -rf target' EXIT

mkdir -p target

GOARCH=amd64 GOOS=linux go build -o target/cloudkarafka-mgmt
cp -r jars target/jars
cp -r static target/static
tar -czf cloudkarafka-mgmt.tar.gz target
rm -rf target
