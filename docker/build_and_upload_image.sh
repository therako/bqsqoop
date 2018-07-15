#!/bin/bash

VERSION=$(cat bqsqoop/__init__.py | awk '{print $3}' | sed s/\"//g)

sed -i '.bkp' s/VERSION/${VERSION}/g ./docker/Dockerfile

docker build . -f ./docker/Dockerfile -t therako/bq-sqoop:${VERSION}
docker push therako/bq-sqoop:${VERSION}
docker build . -f ./docker/Dockerfile -t therako/bq-sqoop:latest
docker push therako/bq-sqoop:latest

mv -f ./docker/Dockerfile.bkp ./docker/Dockerfile
