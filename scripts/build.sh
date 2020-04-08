#!/bin/bash

set -eux

export GOOS=linux

go build -ldflags "-X github.com/cloudkarafka/cloudkarafka-manager/config.GitCommit=$COMMIT -X github.com/cloudkarafka/cloudkarafka-manager/config.Version=$VERSION" -tags static -a -installsuffix cgo -o cloudkarafka-mgmt.linux
go test -v ./...

