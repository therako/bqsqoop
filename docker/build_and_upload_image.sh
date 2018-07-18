#!/bin/bash

VERSION=$(cat bqsqoop/__init__.py | awk '{print $3}' | sed s/\"//g)

sed -i '.bkp' s/VERSION/${VERSION}/g ./docker/Dockerfile

docker build . -f ./docker/Dockerfile -t therako/bqsqoop:${VERSION}
docker push therako/bqsqoop:${VERSION}
docker build . -f ./docker/Dockerfile -t therako/bqsqoop:latest
docker push therako/bqsqoop:latest

mv -f ./docker/Dockerfile.bkp ./docker/Dockerfile
