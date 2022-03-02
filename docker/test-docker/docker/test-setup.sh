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

# This script runs inside the container to prepare the Druid test image.

# Fail fast on any error
set -e

# Fail on unset environment variables
set -u

# Enable for tracing
set -x

# For debugging: verify environment
env

# Install Druid. Some directories aleady exist;
# This will fill in the rest.

DRUID_HOME=/usr/local/druid
cd /usr/local/
tar -xzf usr/local/apache-druid-*-bin.tar.gz

# Add sample data

./start-mysql.sh

java -cp "/usr/local/druid/lib/*" \
	-Ddruid.extensions.directory=${DRUID_HOME}/extensions \
	-Ddruid.extensions.loadList='["mysql-metadata-storage"]' \
	-Ddruid.metadata.storage.type=mysql \
	-Ddruid.metadata.mysql.driver.driverClassName=$MYSQL_DRIVER_CLASSNAME \
	org.apache.druid.cli.Main tools metadata-init \
	--connectURI="jdbc:mysql://localhost:3306/druid" \
	--user=druid --password=diurd

service mysql stop
