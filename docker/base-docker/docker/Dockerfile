# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Builds the base image with everything EXCEPT Druid.
# During development, this image is built once. Then, you can
# rebuild Druid and the Druid image any number of times without
# the overhead of redoing the base work.

# The derived container should install Druid to /usr/local/druid.
# Some files are already in that location: the Druid distribution
# will add to them.

# The result is that this Dockerfile should not depend on your
# Druid build.

# This Dockerfile prefers to use the COPY command over ADD.
# See: https://phoenixnap.com/kb/docker-add-vs-copy

ARG JDK_VERSION=8-slim-buster

# The FROM image provides Java on top of Debian, and
# thus provides bash, apt-get, etc.
# See https://hub.docker.com/_/openjdk

FROM openjdk:$JDK_VERSION

# Dependency versions obtained via Maven, then passed in
# from the pom.xml file. The ENV instruction passes the
# values into the container to be used by the setup script.

ARG KAFKA_VERSION
ENV KAFKA_VERSION=${KAFKA_VERSION}
ARG ZK_VERSION
ENV ZK_VERSION=${ZK_VERSION}
ARG MYSQL_VERSION
ENV MYSQL_VERSION=${MYSQL_VERSION}
ARG MARIA_VERSION
ENV MARIA_VERSION=${MARIA_VERSION}
ARG CONFLUENT_VERSION
ENV CONFLUENT_VERSION=${CONFLUENT_VERSION}
ARG HADOOP_VERSION
ENV HADOOP_VERSION=${HADOOP_VERSION}

# Other options. Normally the defaults are used.

ARG APACHE_ARCHIVE_MIRROR_HOST=https://archive.apache.org
ENV APACHE_ARCHIVE_MIRROR_HOST=${APACHE_ARCHIVE_MIRROR_HOST}

# Choose the driver Must be either com.mysql.jdbc.Driver or
# org.mariadb.jdbc.Driver. See base-setup.sh.

ARG MYSQL_DRIVER_CLASSNAME=com.mysql.jdbc.Driver
ENV MYSQL_DRIVER_CLASSNAME=${MYSQL_DRIVER_CLASSNAME}

# Docker IP as seen from the container. Used to allow services to
# communicate. See base-setup.sh.

ARG DOCKER_IP=127.0.0.1
ENV DOCKER_IP:=${DOCKER_IP}

COPY start-mysql.sh /
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# The single setup script does the setup work.
# Scripts are easier to write than Dockerfiles.
# The entire base image is reused; we don't need
# the intermediate layers in the build cache.

COPY base-setup.sh /root/
RUN bash /root/base-setup.sh

# Expose ports:
# - 8081, 8281: HTTP, HTTPS (coordinator)
# - 8082, 8282: HTTP, HTTPS (broker)
# - 8083, 8283: HTTP, HTTPS (historical)
# - 8090, 8290: HTTP, HTTPS (overlord)
# - 8091, 8291: HTTP, HTTPS (middlemanager)
# - 8888-8891, 9088-9091: HTTP, HTTPS (routers)
# - 3306: MySQL
# - 2181 2888 3888: ZooKeeper
# - 8100 8101 8102 8103 8104 8105: peon ports
# - 8300 8301 8302 8303 8304 8305: peon HTTPS ports
# - 9092, 9093: Kafka (internal, expternal)
EXPOSE 8081 8281
EXPOSE 8082 8282
EXPOSE 8083 8283
EXPOSE 8090 8290
EXPOSE 8091 8291
EXPOSE 3306
EXPOSE 2181 2888 3888
EXPOSE 8100 8101 8102 8103 8104 8105
EXPOSE 8300 8301 8302 8303 8304 8305
EXPOSE 9092 9093

WORKDIR /var/lib/druid
