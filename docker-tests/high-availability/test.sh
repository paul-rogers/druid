#! /bin/bash

SCRIPT_DIR=$(cd $(dirname $0) && pwd)
export ZK_VERSION=3.5.9
export MYSQL_VERSION=5.7-debian
export KAFKA_VERSION=3.1.0
export SHARED_DIR=$SCRIPT_DIR/target/shared

# Dummies just to get the compose files to shut up
export OVERRIDE_ENV=
export DRUID_VERSION=0.23.0-SNAPSHOT

export DRUID_INTEGRATION_TEST_GROUP=test

mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/tasklogs
mkdir -p $SHARED_DIR/db
mkdir -p $SHARED_DIR/kafka
mkdir -p $SHARED_DIR/resources

cp ../assets/log4j2.xml $SHARED_DIR/resources

cd druid-cluster
docker-compose -f docker-compose.test.yaml up -d
