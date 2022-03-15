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

# Fail fast on any error
set -e

# Enable for tracing
set -x

env

# Launch Druid within the container.
cd /

# TODO: enable only for security-related tests?
#/tls/generate-server-certs-and-keystores.sh
. /druid.sh

# Create druid service config files with all the config variables
setupConfig

# Export the service config file path to use in supervisord conf file
DRUID_SERVICE_CONF_DIR="$(. /druid.sh; getConfPath ${DRUID_SERVICE})"

# Export the common config file path to use in supervisord conf file
DRUID_COMMON_CONF_DIR="$(. /druid.sh; getConfPath _common)"

INSTANCE_NAME=$DRUID_SERVICE
if [ -n "$SERVICE_INSTANCE" ]; then
	INSTANCE_NAME=${INSTANCE_NAME}-$SERVICE_INSTANCE
fi
SHARED_DIR=/shared
LOG_DIR=$SHARED_DIR/logs
DRUID_HOME=/usr/local/druid

JAVA_OPTS="$SERVICE_DRUID_JAVA_OPTS $COMMON_DRUID_JAVA_OPTS -XX:HeapDumpPath=$LOG_DIR/$INSTANCE_NAME $DEBUG_OPTS"
LOG4J_CONFIG=$SHARED_DIR/conf/log4j2.xml
if [ -f $LOG4J_CONFIG ]; then
	JAVA_OPTS="$JAVA_OPTS -Dlog4j.configurationFile=$LOG4J_CONFIG"
fi

# The env-to-config scripts creates a single config file.
# The common one is empty, but Druid still wants to find it,
# so we add it to the class path anyway.
CP=$DRUID_COMMON_CONF_DIR:$DRUID_SERVICE_CONF_DIR:${DRUID_HOME}/lib/\*
if [ -n "$DRUID_CLASSPATH" ]; then
	CP=$CP:$DRUID_CLASSPATH
fi
HADOOP_XML=$SHARED_DIR/hadoop-xml
if [ -d $HADOOP_XML ]; then
	CP=$HADOOP_XML:$CP
fi
EXTRA_LIBS=$SHARED_DIR/lib
if [ -d $EXTRA_LIBS ]; then
	CP=$CP:${EXTRA_LIBS}/\*
fi

LOG_FILE=$LOG_DIR/${INSTANCE_NAME}.log
echo "" >> $LOG_FILE
echo "--- Service runtime.properties ---" >> $LOG_FILE
cat $DRUID_SERVICE_CONF_DIR/*.properties >> $LOG_FILE
echo "---" >> $LOG_FILE
echo "" >> $LOG_FILE

# Run Druid service
cd $DRUID_HOME
exec java $JAVA_OPTS -cp $CP \
	org.apache.druid.cli.Main server $DRUID_SERVICE \
	>> $LOG_FILE 2>&1
