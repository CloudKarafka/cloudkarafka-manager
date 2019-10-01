#!/bin/bash

set -eux

export GOOS=linux
export VERSION=0.3.1

go build -ldflags "-X github.com/cloudkarafka/cloudkarafka-manager/config.GitCommit=$COMMIT -X github.com/cloudkarafka/cloudkarafka-manager/config.BuildDate=$(date)" -tags static -a -installsuffix cgo -o cloudkarafka-mgmt.linux
go test -v ./...

