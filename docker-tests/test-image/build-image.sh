#! /usr/bin/env bash
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

# Prepare the image contents and build the Druid image.
# Since Docker requires all contents to be in or below the
# working directory, we assemble the contents in target/docker.

# Fail fast on any error
set -e

# Enable for tracing
set -x

# Fail on unset environment variables
set -u

# Print environment for debugging
env

SCRIPT_DIR=$(cd $(dirname $0) && pwd)

# Maven should have created the docker dir with the needed
# dependency jars. If doing this by hand, run Maven once to
# populate these jars.
if [ ! -d $TARGET_DIR/docker]; then
	echo "$TARGET_DIR/docker does not exist. It should contain dependency jars" 1>&2
	exit 1
fi
mkdir -p $TARGET_DIR/docker
cp -r docker/* $TARGET_DIR/docker
cd $TARGET_DIR/docker

# Grab the distribution.
DISTRIB_FILE=apache-druid-$DRUID_VERSION-bin.tar.gz
SOURCE_FILE=$PARENT_DIR/distribution/target/$DISTRIB_FILE
if [[ ! -f $DISTRIB_FILE || $SOURCE_FILE -nt $DISTRIB_FILE ]]; then
	cp $SOURCE_FILE .
fi

docker build -t $IMAGE_NAME \
	--build-arg DRUID_VERSION=$DRUID_VERSION \
	--build-arg MYSQL_VERSION=$MYSQL_VERSION \
	--build-arg MARIADB_VERSION=$MARIADB_VERSION \
	--build-arg CONFLUENT_VERSION=$CONFLUENT_VERSION \
	--build-arg HADOOP_VERSION=$HADOOP_VERSION \
	--build-arg MYSQL_DRIVER_CLASSNAME=$MYSQL_DRIVER_CLASSNAME \
	.
