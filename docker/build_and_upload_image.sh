#!/bin/bash -e

VERSION=$(cat bqsqoop/__init__.py | awk '{print $3}' | sed s/\"//g)

docker build . -f ./docker/Dockerfile -t therako/bqsqoop:${VERSION}
docker push therako/bqsqoop:${VERSION}
docker build . -f ./docker/Dockerfile -t therako/bqsqoop:latest
docker push therako/bqsqoop:latest
