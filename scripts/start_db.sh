#!/bin/bash

set -e

function quit () {
    echo $2
    exit $1
}

ORACLE_NAME=gemini-oracle
TEST_NAME=gemini-test
GEMINI_CMD=/tmp/gemini

docker-compose --log-level WARNING -f docker-compose.yml up -d

ORACLE_IP=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' ${ORACLE_NAME})
TEST_IP=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' ${TEST_NAME})
SEED=$(date +%s%N)
echo oracle ip: $ORACLE_IP
echo test ip: $TEST_IP
echo "Waiting for ${ORACLE_NAME} to start"
until docker logs ${ORACLE_NAME} 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 2; done
echo "Waiting for ${TEST_NAME} to start"
until docker logs ${TEST_NAME} 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 2; done

#
#$GEMINI_CMD \
#	--duration=10m \
#	--fail-fast \
#	--seed=${SEED} \
#	--dataset-size=small \
#	--test-cluster=${TEST_IP} \
#	--oracle-cluster=${ORACLE_IP} \
#	"$@"
#exit $?
