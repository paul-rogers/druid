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

# Docker Test Image for Druid

This project builds the Docker image for Druid for integration tests.
The Docker compose files use "official" images for dependencies such
as ZooKeeper, MySQL and Kafka. The image contains the Druid distribution,
unpacked, along with the MySQL and Kafka dependencies. Docker Compose is
used to pass configuration specific to each service.

The image here is distinct from the
["retail" image](https://druid.apache.org/docs/latest/tutorials/docker.html)
used for getting started.

## Build Process

Assuming `DRUID_DEV` points to your Druid build directory,
to build the image (only):

```bash
cd $DRUID_DEV/docker-tests/test-image
mvn -P druid-image install
```

Building of the image occurs in four steps:

* The Maven `pom.xml` file gathers versions and other information from the build.
  It also uses the normal Maven dependency mechanism to download the MySQL and
  Kafka client libraries, then copies them to the `target/docker` directory.
  It then invokes the `build-image.sh` script.
* `build-image.sh` adds the Druid build tarball from `distribution/target`
  then invokes the `docker build` command.
* The `docker build` uses `target/docker` as the context, and thus
  uses the `Dockerfile` to build the image. The `Dockerfile` copy artifacts into
  the image, then defers to the `test-setup.sh` script.
* The `test-setup.sh` script is copied into the image and run. This script does
  the work installing Druid.

The resulting image is named `org.apache.druid/test:<version>`.

### Clean

A normal `mvn clean` won't remove the Docker image. Because that is often not
what you want. Instead, do:

```bash
mvn clean -P druid-image
```

### `target/docker`

Docker requires that all build resources be within the current directory. We don't want
to change the source directory: in Maven, only the target directories should contain
build artifacts. So, we build up a `target/docker` directory in `pom.xml` file and the
and `build-image.sh` script:

```text
/target/docker
|- Dockerfile (from docker/)
|- scripts (from docker/)
|- apache-druid-<version>-bin.tar.gz (from distribution, by build-image.sh)
|- MySQL clinet (done by pom.xml)
|- Kafka protobuf client (done by pom.xml)
```

Then, we invoke the `docker build` to build our test image. The `Dockerfile` copies
files into the image. Actual setup is done by the `test-setup.sh` script copied
into the image.

## Debugging

Modern Docker seems to hide the output of commands, which is a hassle to debug
a build. Oddly, the details appear for a failed build, but not for success.
Use the followig to see at least some output:

```bash
export DOCKER_BUILDKIT=0
```

Once the base container is built, you can run it, log in and poke around. First
identify the name. See the last line of the container build:

```text
Successfully tagged org.apache.druid/test:<version>
```

Or ask Docker:

```bash
docker images
```

```bash
docker run --rm -it --entrypoint bash org.apache.druid/test:<version>
```

Quite a few environment variables are provided by Docker and the setup scripts
to see them, use:

```bash
env
```

### Manual Image Builds

You can build the image (only) if you've previously run a full Druid build.
Assume `DRUID_DEV` points to your Druid development root.

```bash
cd $DRUID_DEV/docker/test-docker
mvn -P druid-image install
```

Maven is rather slow to do its part. Let it grind away once to populate
`target/docker`. Then, as you debug the `Dockerfile`, or `test-setup.sh`,
you can build faster:

* Copy `quick-build.example.sh` to `quick-build.sh`.
* Edit the environment variables to set the current versions.
* Invoke `quick-build.sh` to rebuild the image.

When ready, run Maven again to verify all works properly.

### On Each Druid Build

Similarly, if you need to rebuild Druid (because you fixed something), do:

* Do a distribution build of Druid:

```bash
cd $DRUID_DEV
mvn clean package -P dist,skip-static-checks,skip-tests -Dmaven.javadoc.skip=true -T1.0C
```

* Build the test image.

```bash
cd $DRUID_DEV/docker/test-image
mvn -P test-image install
```
### On Each Test Run

* Pick a test "group" to use. Each is in a separate Maven project.

```bash
cd $DRUID_DEV/docker-tests/<test group>
```

* Start a test cluster configured for this test.

```bash
./start-cluster.sh
```

* To run a test from the command line:

```bash
<command needed>
```

* To run from your IDE, find the test to run and start it.

`<instructions needed for TestNG>`

* When done, stop the cluster.

```bash
./stop-cluster.sh
```

<<Add information about debugging Druid in the containers>>

See individual tests for test-specific details.

## Image Contents

The Druid test image adds the following to the base image:

* A Debian base image with the target JDK installed.
* Druid in `/usr/local/druid`
* Druid-related environment variables in `/etc/profile.d/druid-env.sh`
* Test data (TBD)
* Script to run Druid: `/usr/local/run-druid.sh`
* Extra libraries (Kafka, MySQL) placed in the Druid `lib` directory.

The specific "bill of materials" follows. `DRUID_HOME` is the location of
the Druid install and is set to `/usr/local/druid`.

| Variable or Item | Source | Destination |
| -------- | ------ | ----- |
| Druid build | `distribution/target` | `$DRUID_HOME` |
| MySQL Connector | Maven repo | `$DRUID_HOME/lib` |
| Kafka Protobuf | Maven repo | `$DRUID_HOME/lib` |
| Druid launch script | `docker/launch.sh` / `/usr/local/launch.sh` |
| Env-var-to-config script | `docker/druid.sh` | `/usr/local/druid.sh` |

Environment variables defind for both users `root` and `druid`. Most of
these are from the container build process. `DRUID_HOME` is useful at
runtime.

| Name | Description |
| ---- | ----------- |
| `DRUID_HOME` | Location of the Druid install |
| `DRUID_VERSION` | Druid version used to build the image |
| `JAVA_HOME` | Java location |
| `JAVA_VERSION` | Java version |
| `MYSQL_VERSION` | MySQL version (DB, connector) (not actually used) |
| `MYSQL_DRIVER_CLASSNAME` | Name of the MySQL driver (not actually used) |
| `CONFLUENT_VERSION` | Kafka Protobuf library version (not actually used) |

## Configuration

At runtime, two additional sources of information are needed:

* Shared directory (see next section)
* Configuration via environment variables.

Configuration is an extended form of that used by the
[production Docker image](https://druid.apache.org/docs/latest/tutorials/docker.html):
we pass in a large number of environment variables of two kinds:

* General configuration (capitalized)
* Druid configuration file settings (lower case)

We use `docker-compose` to gather up the variables.

These are provided via a `shared` directory mounted into the container.
The shared directory is built in the `target` folder for each test.

The `launch.sh` script fills in a number of implicit configuration items:

| Item | Description |
| ---- | ----------- |
| Heap dump path | Set to `${SHARED}/logs/<instance>` |
| Log4J config | Optional at `${SHARED}/conf/log4j.xml` |
| Hadoop config | Optional at `${SHARED}/hadoop-xml` |
| Extra classpath | `DRUID_CLASSPATH` |
| Java Options | `COMMON_DRUID_JAVA_OPTS` <br/>
`SERVICE_DRUID_JAVA_OPTS` |
| Debug settings | `DEBUG_OPTS` |
| Extra libraries | Optional at `${SHARED}/lib` |
| Extra resources | Optional at `${SHARED}/resources` |

Extra resources is the place to put things like a custom `log4j2.xml`
file.

### General Environment Variables

The required environment variables include:

| Name | Description |
| `DRUID_SERVICE` | Name of the Druid service to run. |
| `DRUID_INSTANCE` | Number/name of an instance when running more than one of
the same kind of service |
| `DRUID_COMMON_JAVA_OPTS` | Java options common to all services. |
| `DRUID_SERVICE_JAVA_OPTS` | Java options for the specific service. |
| `DEBUG_OPTS` | Options to enable debugging. |
| `DRUID_CLASSPATH` | Additional test-specific class path entries.
(See also `$SHARED/lib`.) |

### Service Config Environment Variables

The various `*.env` files provide Druid configuration encoded as environment
variables. Example:

```text
druid_lookup_numLookupLoadingThreads=1
druid_server_http_numThreads=20
```

These are defined in a hierarchy:

* `common.env` - settings common to all services. Acts as defaults.
* `<service>.env` - settings specific to a single service.
* `docker-compose.yaml` - test-specific settings.
* `auth-common.env` - enables security and authorization for the cluster.
  Add this to tests which test security.

The `launch.sh` script converts these variables to config files in
`/tmp/conf/druid`. Those files are then added to the class path.

## Shared Directory

The image assumes a "shared" directory passes in additional configuration
information, and exports logs and other items for inspection.

* Location in the container: `/shared`
* Location on the host: `<project>/target/shared`

This means that each test group has a distinct shared directory,
populated as needed for that test.

Input items:

| Item | Description |
| ---- | ----------- |
| `conf` | `log4j.xml` config (optional) |
| `hadoop-xml` | Hadoop configuration (optional) |
| `hadoop-dependencies` | Hadoop dependencies (optional) |
| `lib` | Extra Druid class path items (optional) |

Output items:

| Item | Description |
| ---- | ----------- |
| `logs` | Log files from each service |
| `tasklogs` | Indexer task logs |
| `kafka` | Kafka persistence |
| `db` | MySQL database |
| `druid` | Druid persistence, etc. |

### Third-Party Logs

The three third-party containers are configured to log to the `/shared`
directory rather than to Docker:

* Kafka: `/shared/logs/kafka.log`
* ZooKeeper: `/shared/logs/zookeeper.log`
* MySQL: `/shared/logs/mysql.log`


## Entry Point

The container launches the `launch.sh` script which:

* Converts environment variables to config files.
* Assembles the Java command line arguments, including those
  explained above, and the just-generated config files.
* Launches Java as "pid 1" so it will receive signals.

Middle Manager launches Peon processes which must be reaped.
Add [the following option](https://docs.docker.com/compose/compose-file/compose-file-v2/#init)
to the Docker Compose configuration for this service:

```text
   init: true
```

## Service Names

The Docker Compose file set up an "overlay" network to connect the containers.
Each is known via a host name taken from the service name. Thus "zookeeper" is
the name of the ZK service and of the container that runs ZK. Use these names
in configuration within each container.

### Host Ports

Outside of the application network, containers are accessible only via the
host ports defined in the Docker Compose files. Thus, ZK is known as `localhost:2181`
to tests and other code running outside of Docker.

## Docker Compose

We use Docker Compose to launch Druid clusters. Each test defines its own cluster
depending on what is to be tested. Since a large amount of the definition is
common, we use inheritance to simplify cluster definition. The base definitions
are in `compose`:

* `common.env` - roughly equivalent to the `_common` configuration area in Druid:
  contains definitions common to all Druid services. Services can override any
  of the definitions.
* `<service>.env` - base definitions for each service, assume it runs stand-alone.
  Adjust if test cluster runs multiple instances.
* `auth.env` - Additional definitions to create a secure cluster. Also requires that
  the client certificates be created.

The set of defind environment variables starts with the
`druid/conf/single-server/micro-quickstart` settings. It would be great to generate
these files directly from the latest quickstart files. For now, it is a manual
process to keep the definitions in sync.

### Define a Test Cluster

To define a test cluster, do the following:

* Define the overlay network.
* Extend the third-party services required (at least ZK and MySQL).
* Extend each Druid service needed. Add a `depends_on` for `zookeeper` and,
  for the Coordinator and Overlord, `metadata`.
* If you need multiple instances of the same service, extend that service
  twice, and define distinct names and port numbers.
* Add any test-specific environment configuration required.

## Exploring the Test Cluster

When run in Docker Compose, the endpoints known to Druid nodes differ from
those needed by a client sitting outside the cluster. We could provide an
explicit mapping. Better is to use the
[Router](https://druid.apache.org/docs/latest/design/router.html#router-as-management-proxy)
to proxy requests. Fortunately, the Druid Console already does this.

## History

This revision of the integration test Docker scripts is based on a prior
integration test version, which is, in turn, based on
the build used for the public Docker image used in the Druid tutorial. If you are familiar
with the prior structure, here are some of the notable changes.

* Use of "official" images for third-party dependencies, rather than adding them
  to the Druid image. (Results in *far* faster image builds.)
* This project splits the prior `druid-integration-tests` project into several parts. This
  project holds the Druid Docker image, while sibling projects hold the cluster definition
  and test for each test group.
  This allows the projects to better utilize the standard Maven build phases, and allows
  better partial build support.
* The prior approach built the Docker image in the `pre-integration-test` phase. Here, since
  the project is separate, we can use the Maven `install` phase.
* The prior structure ran *before* the Druid `distribution` module, hence the Druid artifacts
  were not available, and the scripts did its own build, which could end up polluting the
  Maven build cache. This version runs after `distribution` so it can reuse the actual build
  artifacts.
* The `pom.xml` file in this project does some of the work that that `build_run_cluster.sh`
  previously did, such as passing Maven versions into Docker.
* The work from the prior Dockerfile and `base-setup.sh` are combined into the revised
  `base-setup.sh` here so that the work is done in the target container.
* Since the prior approach was "all-in-one", it would pass test configuration options into
  the container build process so that the container is specific to the test options. This
  project attempts to create a generic container and instead handle test-specific options
  at container run time.
* The detailed launch commands formerly in the Dockerfile now reside in
  `$DRUID_HOME/launch.sh`.
* The prior version used a much-extended version of the public launch script. Those
  extensions moved into `launch.sh` with the eventual goal of using the same launch
  scripts in both cases.
* The various `generate_*_cert.sh` scripts wrote into the source directory. The revised
  scripts write into `target/shared/tls`.
* The shared directory previously was in `~/shared`, but that places the directory outside
  of the Maven build tree. The new location is `$DRUID_DEV/docker/base-docker/target/shared`.
  As a result, the directory is removed and rebuild on each Maven build. The old location was
  removed via scripts, but the new one is very clearly a Maven artifact, and thus to be
  removed on a Maven `clean` operation.
* The prior approach had security enabled for all tests, which makes debugging hard.
  This version makes security optional, it should be enabled for just a security test.
* The orginal design was based on TestNG. Revised tests are based on JUnit.
* The original tests had "test groups" within the single directory. This version splits
  the former groups into projects, so each can have its own tailored cluster definition.
* Prior images would set up MySQL inline in the container by starting the MySQL engine.
  This led to some redundancy (all images would do the same thing) and also some lost
  work (since the DBs in each container are not those used when running.) Here, MySQL
  is in its own image. Clients can update MySQL as needed using JDBC.
* Prior code used Supervisor to launch tasks. This version uses Docker directly and
  runs one process per container (except for Middle Manager, which runs Peons.)

## Future Work

The "public" and "integration test" versions of the Docker images have diverged significantly,
which makes it harder to "test what we ship." Differences include:

* Different base image
* Different ways to set up dependencies.
* Different paths within the container.
* Different launch scripts.
* The test images place Druid in `/usr/local`, the public images in `/opt`.

The tests do want to do things beyond what the "public" image does. However, this should
not require a fork of the builds. To address this issue:

* Extend this project to create a base common to the "public" and integration test images.
* Extend the integration test image to build on top of the public image.
