#!/usr/bin/env bash
set -eu

function usage {
    echo $"Usage: $(basename $0) {production|staging|all}"
    exit 1
}

function build {
    mkdir -p target
    GOARCH=amd64 GOOS=linux go build -o target/cloudkarafka-mgmt.linux
    go build -o target/cloudkarafka-mgmt.amd64.osx
    cp -r static target/static
    tar -czf cloudkarafka-mgmt.tar.gz target
}

function clean {
    rm -rf target
    rm -rf coudkarafka-mgmt.tar.gz
}

function staging {
    aws s3 cp cloudkarafka-mgmt.tar.gz s3://cloudkafka-manager/staging/cloudkarafka-mgmt.tar.gz
}

function production {
    aws s3 cp cloudkarafka-mgmt.tar.gz s3://cloudkafka-manager/production/cloudkarafka-mgmt.tar.gz
}

[ $# -eq 1 ] || usage

case "$1" in
staging)
    build
    staging
    clean ;;
production)
    build
    production
    clean ;;
all)
    build
    staging
    production
    clean ;;
*) usage
esac

