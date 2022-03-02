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

# Launch Druid and other services within the container.

/tls/generate-server-certs-and-keystores.sh
. /druid.sh

# Create druid service config files with all the config variables
setupConfig

# Some test groups require pre-existing data to be setup
setupData

# Export the service config file path to use in supervisord conf file
export DRUID_SERVICE_CONF_DIR="$(. /druid.sh; getConfPath ${DRUID_SERVICE})"

# Export the common config file path to use in supervisord conf file
export DRUID_COMMON_CONF_DIR="$(. /druid.sh; getConfPath _common)"

# Run Druid service using supervisord
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf