# Base Test Docker Image

Druid uses Docker to run integration tests. The Docker image is built in two steps
to speed up debugging. This project builds the base image with everything *except*
Druid. When modifying the base image, the general rule is:

* If the artifact comes from *outside* the Druid source tree, add it here.
* If the artifact comes from *within* the Druid source tree, add it to the
test image.

The above rule speeds debugging: the high cost of downloading extern dependencies
is paid only when they change. The Druid image build is thus much faster.

## Build Process

Building of the image occurs in three steps:

* The Maven `pom.xml` file gathers versions and other information from the build,
  then invokes the `docker build` command.
* The `docker build` uses this project root directory as the context, and thus
  uses the `Dockerfile` to build the image. The Dockerfile mostly sets up options
  and defers to the `base-setup.sh` script.
* The `base-setup.sh` script is copied into the image and run. This script does
  the work of downloading required software (except Druid), preparing the metastore,
  setting up the network and more.

The `base-setup.sh` script has tracing turned on (`set -x`) so that, if anything
fails, you can see what the script was doing. This is vital, as it is very hard
to debug the build script otherwise.

The setup script is left in the container to make it easier to understand what
was done. Find it in `/root/base-setup.sh`.

The resulting image is named `org.apache.druid/test-base:<version>`.

## Debugging

Modern Docker seems to hide the output of commands, which is a hassle to debug
a build. Oddly, the details appear for a failed build, but not for success.
Use the followig to see at least some output:

```bash
export DOCKER_BUILDKIT=0
```

Once the base container is built, you can run it, log in and poke around. First
identify the name:

```
docker images
```

```bash
docker run --rm -it --entrypoint bash org.apache.druid/test-base:<version>
```

Quite a few environment variables are provided by Docker and the setup scripts
to see them, use:

```bash
env
```

Note: be sure to use `bash` and not `sh`: some environment variables
are set only for `bash`.

## Image Contents

The main image contents include:

* A Debian base image with the target JDK installed.
* `wget`, `supervise` and `mysql` installed from the Debian repos.
* Zookeeper, Kafka and the MySQL (or MariaDB) driver from their respective
  repos.
* Supervisor scripts for the above in `/usr/lib/druid/conf/`.

Key locations:

* `ZK_HOME`: `/usr/local/zookeeper`
* `KAFKA_HOME`: `/usr/local/kafka`

For convenience, these variables are defined in `/etc/profile.d/druid-env.sh`
which is read by `bash` on startup, and thus available to all users.

The usual commands for these tools are available in the expected
directories.

The Druid test image adds Druid in this location:

* `DRUID_HOME`: `/usr/local/druid`

In all three cases, the software appears in a versioned directory, and is
symlinked to the generic name:

```text
kafka -> /usr/local/kafka_2.13-3.1.0
zookeeper -> /usr/local/apache-zookeeper-3.5.9-bin
```

And the Druid image adds:

```text
druid -> /usr/local/apache-druid-0.23.0-SNAPSHOT
```

The Zookeeper and MySql client libraries are downloaded. Since there no Druid
available yet, they are left in `/usr/local/druid-libs` for merging into the
Druid install when that is done later.

### Runtime Dependencies

To a service, the launch script should mount an external volume to `/shared`
to capture logs, etc.

Logs:

* Kafka: `/shared/logs/kafka.log`
* ZooKeeper: `/shared/logs/zookeeper.log`
* MySQL: `/shared/logs/mysql.log`

### Druid Launch

The `/usr/lib/druid/conf/druid.conf` file launches Druid. It requires the following
to be set:

* `DRUID_SERVICE_JAVA_OPTS` - Java options
* `DRUID_COMMON_JAVA_OPTS` - More Java optoins
* `DRUID_COMMON_CONF_DIR` - Common config dir on the class path
* `DRUID_SERVICE_CONF_DIR` - Service-specifi config dir on the class path
* `DRUID_DEP_LIB_DIR`
* `DRUID_SERVICE` - Name of the Druid service to run
* `DRUID_LOG_NAME` - Log path in `/shared/logs`


### MySQL

Previous versions of the Druid integration tests included MySQL into the image.
However, it is easier and more reliable to use the
[official image](https://hub.docker.com/_/mysql).

The Druid database is created on initial startup in `$SHARED_DIR/db`.

The following is old:

MySQL is a bit special. To start MySQL, use:

```bash
/usr/local/start-mysql.sh
```

To stop MySQL, use:

```bash
service mysql stop
```

### ZooKeeper

Previous versions of the Druid integration tests included ZK into the image.
However, it is easier and more reliable to use the
[official image](https://hub.docker.com/_/zookeeper).

### Ports

The image exposes a large collection of ports, see the `Dockerfile` for the list.
The ports used here are the standard Druid ports. This means that you cannot run
this container while a local Druid cluster is up or visa-versa.

## Support Files

This project also creates a number of support files in a "shared" directory. "Shared"
in the sense that the directory is visible both to the client and is mounted into
containers.

## History

This revision of the Docker scripts is based on a prior version, which is, in turn, based on
the build used for the public Docker image used in the Druid tutorial. If you are familiar
with the prior structure, here are some of the notable changes.

* This project splits the prior `druid-integration-tests` project into several parts. This
  project holds the base image, while `test-docker` holds the image with Druid software.
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
