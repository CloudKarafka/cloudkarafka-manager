#!/bin/bash

set -eux

mkdir -p target
cp cloudkarafka-mgmt.linux target/cloudkarafka-mgmt.linux
cp -r static target/static
tar -czf cloudkarafka-mgmt.tar.gz target

if [ "$BRANCH" = "stable" ]; then
	aws s3 cp cloudkarafka-mgmt.tar.gz s3://cloudkafka-manager/production/cloudkarafka-mgmt.tar.gz --region us-east-1
	aws s3 cp cloudkarafka-mgmt.tar.gz s3://cloudkafka-manager/staging/cloudkarafka-mgmt.tar.gz --region us-east-1
fi;

if [ "$BRANCH" = "master" ]; then
	aws s3 cp cloudkarafka-mgmt.tar.gz s3://cloudkafka-manager/staging/cloudkarafka-mgmt.tar.gz --region us-east-1
fi;

aws s3 cp cloudkarafka-mgmt.tar.gz "s3://cloudkafka-manager/cloudkarafka-mgmt-$BRANCH.tar.gz" --region us-east-1
