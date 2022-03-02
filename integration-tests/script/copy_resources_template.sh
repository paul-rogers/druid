#!/usr/bin/env bash
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

# This script assumes that the working directory is
# $DRUID_DEV/druid-integration-tests

echo "Copying integration test resources."

set -e

# Setup client keystore
./docker/tls/generate-client-certs-and-keystores.sh
rm -rf docker/client_tls
cp -r client_tls docker/client_tls

if [ -z "$SHARED_DIR" ]; then
  # Avoid deleting and creating the wrong directory.
  echo "SHARED_DIR is not set!" >&2
  exit 1
fi
if [ -z "$DRUID_VERSION" ]; then
  echo "DRUID_VERSION is not set!" >&2
  exit 1
fi

# Setup client keystore
rm -rf $SHARED_DIR/docker
mkdir -p $SHARED_DIR
cp -R docker $SHARED_DIR/docker

if [ $DRUID_REUSE_BUILD -eq 1 ]; then
  echo "Resusing existing build"
  it_dir=../distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin
  if [ ! -d $it_dir ]; then
  	echo "IT Build directory does not exist:" $it_dir >&2
  	exit 1
  fi
  if [ ! -d $it_dir/lib ]; then
  	echo "IT Build directory does not exist:" $it_dir/lib >&2
  	exit 1
  fi
  if [ ! -d $it_dir/extensions ]; then
  	echo "IT Build directory does not exist:" $it_dir/extensions >&2
  	exit 1
  fi
  cp -R $it_dir/lib $SHARED_DIR/docker/lib
  cp -R $it_dir/extensions $SHARED_DIR/docker/extensions
else
  echo "Running maven build"
  pushd ../
  rm -rf distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin
  mvn -DskipTests -T1C -Danimal.sniffer.skip=true -Dcheckstyle.skip=true -Ddruid.console.skip=true -Denforcer.skip=true -Dforbiddenapis.skip=true -Dmaven.javadoc.skip=true -Dpmd.skip=true -Dspotbugs.skip=true install -Pintegration-test
  mv distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/lib $SHARED_DIR/docker/lib
  mv distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/extensions $SHARED_DIR/docker/extensions
  popd
fi

# Make directories if they don't exist
# (They won't exist because we removed the shared dir above.)

mkdir -p $SHARED_DIR/hadoop_xml
mkdir -p $SHARED_DIR/hadoop-dependencies
mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/tasklogs
mkdir -p $SHARED_DIR/docker/extensions
mkdir -p $SHARED_DIR/docker/credentials

# Install logging config

cp src/main/resources/log4j2.xml $SHARED_DIR/docker/lib/log4j2.xml

# Extensions for testing are pulled while creating a binary.
# See the 'integration-test' profile in $ROOT/distribution/pom.xml.

# Pull Hadoop dependency if needed
if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
then
  ## We put same version in both commands but as we have an if, correct code path will always be executed as this is generated script.
  ## <TODO> Remove if
  mkdir -p $SHARED_DIR/hadoop-dependencies/hadoop-gcs
  if [ -n "${HADOOP_VERSION}" ] && [ "${HADOOP_VERSION:0:1}" == "3" ]; then
    java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client-api:${hadoop.compile.version} -h org.apache.hadoop:hadoop-client-runtime:${hadoop.compile.version} -h org.apache.hadoop:hadoop-aws:${hadoop.compile.version} -h org.apache.hadoop:hadoop-azure:${hadoop.compile.version}
    curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --output $SHARED_DIR/hadoop-dependencies/hadoop-gcs/gcs-connector-hadoop3-latest.jar
  else
    java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client:${hadoop.compile.version} -h org.apache.hadoop:hadoop-aws:${hadoop.compile.version} -h org.apache.hadoop:hadoop-azure:${hadoop.compile.version}
    curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar --output $SHARED_DIR/hadoop-dependencies/hadoop-gcs/gcs-connector-hadoop2-latest.jar
  fi
fi

# One of the integration tests needs the wikiticker sample data

mkdir -p $SHARED_DIR/wikiticker-it
cp ../examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz $SHARED_DIR/wikiticker-it/wikiticker-2015-09-12-sampled.json.gz
cp docker/wiki-simple-lookup.json $SHARED_DIR/wikiticker-it/wiki-simple-lookup.json
cp docker/test-data/wikipedia.desc $SHARED_DIR/wikiticker-it/wikipedia.desc

# Copy other files if needed

if [ -n "$DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH" ]
then
  cp -a $DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH/. $SHARED_DIR/docker/credentials/
fi
