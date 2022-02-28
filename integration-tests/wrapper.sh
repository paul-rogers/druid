# !/bin/bash

# Allows you to run the IT Docker steps one at a time.
# Run this from the $DRUID_DEV/integration-tests directory
# since the scripts often use paths relative to this directory.
#
# Commands
# --------
#
# mvn:
#     Do an integration test build, but without the static checks
#     or running any tests. Do this if the sources change or if
#     you've not done your own build.
# copy:
#     Create the ~/shared directory mounted into the containers.
#     This directory contains many resources used by the containers,
#     but not included into the containers themselves during build.
#      Look at the contents of ~/shared to see 
#     Run this once per debug session unless something changes.
# build:
#     Builds the containers. Run once per session unless something
#     changes.
# all:
#     Do all of the above. This is just like running from Maven,
#     but without the Maven build steps.
#
# Env Vars
# --------

# You must first set up the environment variables as shown below.

# Env vars. Captured from Maven. Add the following line
# to the top of build_run_cluster.sh:
# env > /tmp/env.out
# Let Maven do its thing. Then use the value of that
# file to adjust the entries here.

. versions.sh

# The copy script normally runs a Maven build, then detroys the
# build artifacts during copying. This variable says that you
# did your own build, thank you, and to please not delete your files.
export DRUID_REUSE_BUILD=1

# Setup copied from build_run_cluster.sh

echo $DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH

export DIR=$(cd $(dirname $0) && pwd)
export HADOOP_DOCKER_DIR=$DIR/../examples/quickstart/tutorial/hadoop/docker

if [ -n "${HADOOP_VERSION}" ] && [ "${HADOOP_VERSION:0:1}" == "3" ]; then
   export HADOOP_DOCKER_DIR=$DIR/../examples/quickstart/tutorial/hadoop3/docker
fi

export DOCKERDIR=$DIR/docker
export SHARED_DIR=${HOME}/shared

# so docker IP addr will be known during docker build
echo ${DOCKER_IP:=127.0.0.1} > $DOCKERDIR/docker_ip

# Do the requested step

if [ $# -eq 0 ]; then
  cmd=all
else
  cmd=$1
fi

if [[ $cmd == 'all' || $cmd == 'mvn' ]]; then
  echo "Maven build for integration tests"
  set -x
  mvn install -P integration-test -DskipTests -T1C -Danimal.sniffer.skip=true \
  -Dcheckstyle.skip=true -Ddruid.console.skip=true -Denforcer.skip=true -Dforbiddenapis.skip=true \
  -Dmaven.javadoc.skip=true -Dpmd.skip=true -Dspotbugs.skip=true -nsu 
  set +x
fi

if [[ $cmd == 'all' || $cmd == 'copy' ]]; then
  echo "Copy resources"
  echo "This creates the folder shared into containers: ~/shared"
  # Use the "template" rather than depend on the trivial generated copy.
  ./scripts/copy_resources_template.sh
  echo "Copying done"
fi

if [[ $cmd == 'all' || $cmd == 'build' ]]; then
  echo "Build containers"
  ./script/docker_build_containers.sh
fi
