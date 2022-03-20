<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Integration Tests (Revised)

This project, and its subprojects, build a Docker image for Druid,
then use that image, along with test configuration to run tests.

This version greatly evolves the integration tests from the earlier
form. See the last section for details.

See `test-image/README.md` for details on the Docker image.

## Goals

The goal of the present version is to simplify development.

* Speed-up the Druid test image build by avoiding download of
  dependencies. (Instead, any such dependencies are managed by
  Maven and reside in the local build cache.)
* Use official images for dependencies to avoid the need to
  download, install, and manage those dependencies.
* Ensure that it is easy to manually build the image, launch
  a cluster, and run a test against the cluster.
* Convert tests to JUnit so that they will easily run in your
  favorite IDE, just like other Druid tests.
* Use the actual Druid build from `distribution` so we know
  what is tested.
* Leverage, don't fight, Maven.
* Run the integration tests easily on a typical development machine.

If these goals are met, then you should be able to quickly:

* Build the Druid distribution.
* Build the Druid image.
* Launch the cluster for the particular test.
* Run the test any number of times in your debugger.
* Clean up the test artifacts.

The result is that the fastest path to trying our your code
changes is:

* Create a normal unit test and run it to verify your code.
* Create an integration test that double-checks the code in
  a live cluster.

The result should also speed up Travis builds:

* A single Maven run can produce the Druid artifacts and run
  all integration tests.

We can have the option to run the integration tests using different
settings: one with the MariaDB connector, say, another with MySQL.
Each of these might be a separate Travis run, since the tests run
with different configurations (as given by a Maven profile). But,
we won't have to run a separte Travis job for each integration test
group.

## Structure

This module is a collection of sub-modules:

* `testing-tools`: Testing extensions added to the Docker image.
* `test-image`: Builds the Druid test Docker image.
* `base-test`: Test code used by all tests.
* `<group>`: The remaining projects represent test groups: sets of
  tests which use the same Cluster configuration. A new group is created
  when test need a novel cluster configuration.

This module runs *after* the Druid distribution is built, so that we
create the Docker image using the same build artifacts that we "ship".

This module has a number of folders that are used by all tests:

* `compose`: A collection of Docker Compose scripts that define the basics
  of the cluster. Each test "inherits" the bits that it needs.
* `compose/environment-configs`: Files which define, as environment variables,
  the runtime properties for each service in the cluster. (See below
  for details.)
* `assets`: The `log4j2.xml` file used by images for logging.

## Test Execution

The basic process for running a test group (sub-module) is:

* The test builds a `target/shared` directory with items to be mounted
  into the containers, such as the `log4j2.xml` file, sample data, etc.
  The shared directory also holds log files, Druid persistent storage,
  the metastore (MySQL) DB, etc. See `test-image/README.md` for details.
* The test is configured via a `druid-cluster/compose.yaml` file.
  This file defines the services to run and their configuration.
* The `start-cluster.sh` script builds the shared directory and starts
  the cluster.
* Tests run on the local host within JUnit.
* The `Initialization` class loads the cluster configuration (see below),
  optionally populates the Druid metadata storage, and is used to
  inject instances into the test.
* The individual tests run.
* The `stop-cluster.sh` script shuts down the cluster.

## Test Configuration

To create a test, you must supply at least three key components:

* A `druid-cluster/docker-compose.yaml` file that launches the desired cluster.
  (The folder name `druid-cluster` becomes the application name in Docker.)
* A `src/test/resources/yaml/cluster.yaml` file that describes the cluster
  for tests. This file can also include Metastore SQL statements needed to
  populate the metastore.
* The test itself, as a JUnit test that uses the `Initializer` class to
  configure the tests to match the cluster.

### `docker-compose.yaml`

A typical `docker-compose.yaml` file picks a set of Druid services to run.

* The services are mosty defined in the `compose/dependencies.yaml` and
  `compose/druid.yaml` files under this directory. The test-specific file
  inherits these definitions as needed.
* If a test wants to run two of some service (say Coordinator), then it
  can use the "standard" definition for only one of them and must fill in
  the details (especially distinct port numbers) for the second.
* By default, the container and internal host name is the same as the service
  name. Thus, a `broker` service resides in a `broker` container known as
  host `broker` on the Docker overlay network.
* The service name is also usually the log file name. Thus `broker` logs
  to `/target/shared/logs/broker.log`.
* An environment variable `DRUID_INSTANCE` adds a suffix to the service
  name and causes the log file to be `broker-one.log` if the instance
  is `one`. The service name should have the full name `broker-one`.
* Druid configuration comes from the common and service-specific environment
  files in `/compose/environment-config`. A test-specific service configuration
  can override any of these settings using the `environment` section.
* For special cases, the service can define its configuration in-line and
  not load the standard settings at all.
* Each service can override the Java options. However, in practice, the
  only options that actually change are those for memory. As a result,
  the memory settings reside in `DRUID_SERVICE_JAVA_OPTS`, which you can
  easily change on a service-by-service or test-by-test basis.
* Debugging is enabled on port 8000 in the container. Each service that
  wishes to expose debugging must map that container port to a distinct
  host port.

The easiest understand the above is to look at a few examples.

### `cluster.yaml`

Tests typically need to understand how the cluster is structured. The
`src/test/resources/yaml/cluster.yaml` file in each test does this. The
file holds a subset of the Docker Compose inforamtion, along with other
setup.

* Services: every service offers the possibility of multiple service
  instances, though only a few services actually need this feature.
* `zk` represents ZooKeeper, running in its "official" container.
  See `ZKConfig` class for the available config settings.
* `kafka` describes Kafka, also running in an official container.
  See `KafkaConfig` for the available config settings.
* `metastore` describes the Druid "metadata storage" (metastore) typically
  hosted in the offical MySql container. See `MetastoreConfig` for
  configuration options.
* `druid` describes the Druid cluster as a map of service names. Use
  the standard names defined in `ClusterConfig`. The nodes are given
  in the `instances` section.
* Use the `tag` element of the instance when running multiple Druid
  services of the same type. `tag` should match `DRUID_INSTANCE` in
  Docker Compose.
* `metastoreInit` is a set of MySQL statements to be run against the
  metadata storage before the test starts. Ensure each is idempotent to
  allow running tests multiple times against the same target directory.

Configuration provides a number of inheritance rules.

* The container and host names are assumed to be the same as the service
  unless overridden.
* Local host ports are assumed to be the same as the internal container
  ports, unless overriden with the `hostPort` element.
* Local host URLs are assumed to be on the Docker host, which defaults
  to `localhost`.

The result is that you can specify a minimum amount of information in
`cluster.yaml`. Again, see the examples for more information.

## Debugging

Ease of debugging is a key goal of the revised structure.

* Rebuild the Docker image only when the Druid code changes.
  Do a normal distribution build, then build a new image.
* Reuse the same image over and over if you only change tests
  (such as when adding a new test.)
* Reuse the same `shared` directory when the test does not
  make permanent changes.
* Change Druid configuration by changing the Docker compose
  files, no need to rebuild the image.
* Work primarily in the IDE when debugging tests.
* To add more logging, change the `log4j2.xml` file in the shared
  directory to increase the logging level.
* Remote debug Druid services if needed.

## Next Steps

The present version is very much a work in progress. Work completed to
date includes:

* Restructure the Docker images to use the Druid produced from the
  Maven build. Use "official" images for dependencies.
* Restructure the Docker compose files.
* Create the cluster configuration mechanisms.
* Convert one "test group" to a sub-module: "high-availability".
* Create the `pom.xml`, scripts and other knick-knacks needed to tie
  everything together.
* Create the initial test without using security settings to aid
  debugging.

However, *much* work remains:

* Convert remaining tests.
* Decide when we need full security. Convert the many certificate
  setup scripts.

## History

* The prior tests required a separate Docker build for each test "group"
  Here, the former groups are sub-projects. All use the same Docker image.
* The prior code used the long-obsolte TestNG. Tests here use JUnit.
* The prior test used a TestNG suite to create test intances and inject
  various items using Guice. This version uses an `Initializer` class to
  do roughly the same job.
* The prior tests required test configuration be passed in on the command
  line, which is tedious when debugging. This version uses a cluster
  configuation file instead.
* The prior version perfomed MySQL initialization in the Docker container.
  But, since each test would launch multiple containers, that work was
  done multiple times. Here the work is done by the test itself.
* The prior version had a single "shared" directory for all tests in
  `~/shared`. This version creates a separate shared folder for each
  test module, in `<module>/target/shared`. This ensures that Maven will
  delete everything between test runs.
* This version removes many of the `druid-` prefixes on the container
  names. We assume that the cluster runs as the only Docker app locally,
  so the extra naming just clutters things.
