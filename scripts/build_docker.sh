#!/bin/bash
set -e
cd  $(dirname ${BASH_SOURCE})/..
export GEMINI_VERSION=$(poetry version -s)
export IMAGE=scylladb/hydra-loaders:gemini-python-$GEMINI_VERSION
docker build -t $IMAGE --build-arg version=$GEMINI_VERSION .
echo To push built image run:
echo docker push $IMAGE
echo Example execution:
echo docker run --network=host --rm $IMAGE gemini -t 192.168.100.2 -o 192.168.100.3
