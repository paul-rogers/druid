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

if [ -z "$DRUID_VERSION" ]; then
	echo "DRUID_VERSION is not set" 2>&1
	exit 1
fi

# Install Druid, owned by user:group druid:druid
# The original Druid directory contains only
# libraries. No extensions should be present: those
# should be added in this step.

DRUID_HOME=/usr/local/druid
cd /usr/local/

# TODO: Skip this once base uses the correct name
mv druid druid-lib

tar -xzf apache-druid-${DRUID_VERSION}-bin.tar.gz
rm apache-druid-${DRUID_VERSION}-bin.tar.gz

# Leave the versioned directory, create a symlink to $DRUID_HOME.
chown -R druid:druid apache-druid-${DRUID_VERSION}
ln -s apache-druid-${DRUID_VERSION} $DRUID_HOME

# Merge libs provided by the base image
mv druid-lib/lib/* druid/lib

# Remove step-by-step. Will fail if the base adds more items
# that this script does not yet handle.
rmdir druid-lib/lib
# TODO: Remove this line
rmdir druid-lib/extensions
rmdir druid-lib

# Convenience script to run Druid for tools.
# Expands the env vars into the script for stability.
cat > run-druid.sh << EOF
#! /bin/bash

java -cp "${DRUID_HOME}/lib/*" \\
	-Ddruid.extensions.directory=${DRUID_HOME}/extensions \\
	-Ddruid.extensions.loadList='["mysql-metadata-storage"]' \\
	-Ddruid.metadata.storage.type=mysql \\
	-Ddruid.metadata.mysql.driver.driverClassName=$MYSQL_DRIVER_CLASSNAME \\
	\$*
EOF
chmod a+x run-druid.sh

# TODO: Temporary
ln -s /start-mysql.sh start-mysql.sh

# Initialize metadata

./start-mysql.sh

./run-druid.sh \
	org.apache.druid.cli.Main tools metadata-init \
	--connectURI="jdbc:mysql://localhost:3306/druid" \
	--user=druid --password=diurd

service mysql stop

# Add Druid-related environment info. Only need those defined here:
# Docker provides those defined at build time.

cat >> druid-env.sh << EOF
export DRUID_HOME=$DRUID_HOME
EOF

# TODO: Temporary
cat >> /root/.bashrc << EOF
source /usr/local/druid-env.sh
EOF

