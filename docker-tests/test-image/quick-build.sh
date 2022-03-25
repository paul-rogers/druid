#! /bin/bash

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

# Example file to build the docker image outside of Maven for
# debugging. Maven sets environment variables, then calls
# build-image.sh. Here we set those environment variables by
# hand. This is an example because you should copy this script
# to quick-build.sh and set the actual version numbers you
# need.

# Build version. Maven ${project.version}
export DRUID_VERSION=0.23.0-SNAPSHOT
export MYSQL_VERSION=5.1.48
export CONFLUENT_VERSION=5.5.1
export MARIADB_VERSION=2.7.3
export HADOOP_VERSION=2.8.5
export MYSQL_DRIVER_CLASSNAME=com.mysql.jdbc.Driver

# The following are pretty standard. Change only if your
# setup is unusual.

SCRIPT_DIR=$(cd $(dirname $0) && pwd)

# Target directory. Maven ${project.build.directory}
# Example is for the usual setup.
export TARGET_DIR=$SCRIPT_DIR/target

# Directory of the parent Druid pom.xml file.
# Unbeliebably hard to get from Maven itself.
export PARENT_DIR=$SCRIPT_DIR/../..

# Name of the image.
export IMAGE_NAME=org.apache.druid/test:$DRUID_VERSION

./build-image.sh
