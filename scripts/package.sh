#!/bin/bash

set -eux

mkdir -p target
cp cloudkarafka-mgmt.linux target/cloudkarafka-mgmt.linux
cp -r static target/static
tar -czf cloudkarafka-mgmt.tar.gz target
