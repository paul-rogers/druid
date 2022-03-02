#! /usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script runs inside the container to prepare the base image.

# Fail fast on any error
set -e

# Fail on unset environment variables
set -u

# Enable for tracing
set -x

# For debugging: verify environment
env

# Do work in /root to make it easy to identify unwanted
# left-overers.

cd /root

# Druid system user

adduser --system --group --no-create-home druid

# Create the stub Druid install directory. The Druid
# image adds the Druid software.
# Would be great for these artifacts to live outside Druid,
# but Druid can't handle such a setup at present.

DRUID_HOME=/usr/local/druid
mkdir -p ${DRUID_HOME}
mkdir -p ${DRUID_HOME}/extensions
mkdir -p ${DRUID_HOME}/lib
chown -R druid:druid $DRUID_HOME

export DEBIAN_FRONTEND=noninteractive
apt-get update

# wget
apt-get install -y wget

# MySQL (Metadata store)
# Installed as a service, see below for an example of how to start MySQL.

apt-get install -y default-mysql-server

echo "[mysqld]\ncharacter-set-server=utf8\ncollation-server=utf8_bin\n" >> /etc/mysql/my.cnf

# Supervisor
apt-get install -y supervisor

# Zookeeper

ZK_HOME=/usr/local/zookeeper
ZK_TAR=apache-zookeeper-$ZK_VERSION-bin
wget -q -O /tmp/$ZK_TAR.tar.gz "$APACHE_ARCHIVE_MIRROR_HOST/dist/zookeeper/zookeeper-$ZK_VERSION/$ZK_TAR.tar.gz"
tar -xzf /tmp/$ZK_TAR.tar.gz -C /usr/local
cp /usr/local/$ZK_TAR/conf/zoo_sample.cfg /usr/local/$ZK_TAR/conf/zoo.cfg
rm /tmp/$ZK_TAR.tar.gz
ln -s /usr/local/$ZK_TAR $ZK_HOME

# Kafka
# KAFKA_VERSION is defined by docker build arguments

KAFKA_HOME=/usr/local/kafka
wget -q -O /tmp/kafka_2.13-$KAFKA_VERSION.tgz "$APACHE_ARCHIVE_MIRROR_HOST/dist/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"
tar -xzf /tmp/kafka_2.13-$KAFKA_VERSION.tgz -C /usr/local
ln -s /usr/local/kafka_2.13-$KAFKA_VERSION $KAFKA_HOME
rm /tmp/kafka_2.13-$KAFKA_VERSION.tgz

# Download the MySQL Java connector
# target path must match the exact path referenced in environment-configs/common
# alternatively: Download the MariaDB Java connector, and pretend it is the mysql connector

if [ "$MYSQL_DRIVER_CLASSNAME" = "com.mysql.jdbc.Driver" ]; then
    wget -q "https://repo1.maven.org/maven2/mysql/mysql-connector-java/$MYSQL_VERSION/mysql-connector-java-$MYSQL_VERSION.jar" \
    	 -O ${DRUID_HOME}/lib/mysql-connector-java.jar
elif [ "$MYSQL_DRIVER_CLASSNAME" = "org.mariadb.jdbc.Driver" ]; then
    wget -q "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/$MARIA_VERSION/mariadb-java-client-$MARIA_VERSION.jar" \
    	 -O ${DRUID_HOME}/lib/mysql-connector-java.jar
else
	echo "No MySQL Driver specified. Set MYSQL_DRIVER_CLASSNAME correctly."
	exit 1
fi

# Download kafka protobuf provider

wget -q "https://packages.confluent.io/maven/io/confluent/kafka-protobuf-provider/$CONFLUENT_VERSION/kafka-protobuf-provider-$CONFLUENT_VERSION.jar" \
     -O ${DRUID_HOME}/lib/kafka-protobuf-provider.jar

# Setup metadata store

/start-mysql.sh

echo "CREATE USER 'druid'@'%' IDENTIFIED BY 'diurd'; \
      GRANT ALL ON druid.* TO 'druid'@'%'; \
      CREATE database druid DEFAULT CHARACTER SET utf8mb4;" | mysql -u root \

service mysql stop

# internal docker_ip:9092 endpoint is used to access Kafka from other Docker containers
# external docker ip:9093 endpoint is used to access Kafka from test code

perl -pi -e "s/#listeners=.*/listeners=INTERNAL:\/\/172.172.172.2:9092,EXTERNAL:\/\/172.172.172.2:9093/" /usr/local/kafka/config/server.properties
perl -pi -e "s/#advertised.listeners=.*/advertised.listeners=INTERNAL:\/\/172.172.172.2:9092,EXTERNAL:\/\/${DOCKER_IP}:9093/" /usr/local/kafka/config/server.properties
perl -pi -e "s/#listener.security.protocol.map=.*/listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT\ninter.broker.listener.name=INTERNAL/" /usr/local/kafka/config/server.properties

# Create a setup file with the various paths

echo /.bashrc << EOF
export DRUID_HOME=$DRUID_HOME
export ZK_HOME=$ZK_HOME
export KAFKA_HOME=$KAFKA_HOME
export MYSQL_DRIVER_CLASSNAME=$MYSQL_DRIVER_CLASSNAME
EOF

# clean up time

apt-get clean
rm -rf /tmp/*
rm -rf /var/tmp/*
rm -rf /var/lib/apt/lists
rm -rf /root/.m2
