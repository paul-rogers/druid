#! /bin/bash

SCRIPT_DIR=$(cd $(dirname $0) && pwd)
export ZK_VERSION=3.5.9
export MYSQL_VERSION=5.7-debian
export SHARED_DIR=$SCRIPT_DIR/target/shared

mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/db

cd docker
docker-compose -f docker-compose.test.yml up -d
