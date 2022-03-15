# Druid Test Docker Image

This project builds the Docker image for Druid for integration tests.
The Docker compose files use "official" images for dependencies such
as ZooKeeper, MySQL and Kafka. The image contains the Druid distribution,
unpacked, along with the MySQL and Kafka dependencies. Docker Compose is
used to pass configuration specific to each service.

The image here is distinct from the
["retail" image](https://druid.apache.org/docs/latest/tutorials/docker.html)
used for getting started.

## Build Process

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
mvn clean -P base-image -P test-image
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
SERVICE_DRUID_JAVA_OPTS |
| Debug settings | `DEBUG_OPTS` |
| Extra libraries | Optional at `${SHARED}/lib` |

### General Environment Variables

The required environment variables include:

| Name | Description |
| `DRUID_SERVICE` | Name of the Druid service to run. |
| `INSTANCE_NAME` | Number/name of an instance when running more than one of
the same kind of service |
| `COMMON_DRUID_JAVA_OPTS` | Java options common to all services. |
| `SERVICE_DRUID_JAVA_OPTS` | Java options for the specific service. |
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
