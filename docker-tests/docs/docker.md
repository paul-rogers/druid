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

Integration test need a Druid cluster. While some tests support using
Kubernetes or the Quickstart cluster, most need a cluster with some
test-specfic configuration. We use Docker Compose to create that cluster,
based on a test-oriented Docker iamge built by the `test-image` project.
The image contains the Druid distribution,
unpacked, along with the MySQL and Kafka dependencies. Docker Compose is
used to pass configuration specific to each service.

In addition to the Druid image, we use "official" images for dependencies such
as ZooKeeper, MySQL and Kafka.

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
