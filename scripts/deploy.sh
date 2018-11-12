#!/bin/bash

set -eux

mkdir -p target
cp cloudkarafka-mgmt.linux target/cloudkarafka-mgmt.linux
cp -r static target/static
tar -czf cloudkarafka-mgmt.tar.gz target
export TARGET=$TRAVIS_BRANCH
if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    export TARGET="$TARGET-$TRAVIS_PULL_REQUEST"
fi;
echo "Deploying to bucket: $TARGET"
aws s3 cp cloudkarafka-mgmt.tar.gz "s3://cloudkafka-manager/$TARGET/cloudkarafka-mgmt.tar.gz" --region us-east-1

