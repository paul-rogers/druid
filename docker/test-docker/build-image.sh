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

if [ $# -lt 1 ]; then
	echo "Usage: build-image.sh <druid-version>" 2>&1
	exit 1
fi

# Fail on unset environment variables
set -u

DRUID_VERSION=$1

rm -Rf target/docker
mkdir -p target/docker
cp -r docker/* target/docker

# Grab the distribution. Rename it so that the build script
# doesn't have to mess with versions (much).
cp ../../distribution/target/apache-druid-$DRUID_VERSION-bin.tar.gz target/docker/
cd target/docker

# TODO: Rename base image to test-base.
docker build -t org.apache.druid/test:$DRUID_VERSION \
	--build-arg DRUID_VERSION=$DRUID_VERSION \
	.