# Base Test Docker Image

Druid uses Docker to run integration tests. The Docker image is built in two steps
to speed up debugging. This project builds the base image with everything *except*
Druid.

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
fails, you can see what the script was doing.

The setup script is left in the container to make it easier to understand what
was done. Find it in `/root/base-setup.sh`.

## Debugging

Once the base container is build, you can run it, log in and poke around. First
identify the name:

```
docker images
```

The name will be of the form `org.apache.druid/base-docker:<version>`

```bash
docker run --rm -it --entrypoint bash org.apache.druid/base-docker:<version>
```

## Image Contents

The main image contents include:

* A Debian base image with the target JDK installed.
* `wget`, `supervise` and `mysql` installed from the Debian repos.
* Zookeeper, Kafka and the MySQL (or MariaDB) driver from their respective
  repos.

Key locations:

* `DRUID_HOME`: `/usr/local/druid`
* `ZK_HOME`: `/usr/local/zookeeper`
* `KAFKA_HOME`: `/usr/local/kafka`

For convenience, these variables are defined in `/.bashrc`:

```bash
source /.bashrc
```

The usual commands for these tools are available in the expected
directories.

### MySQL

MySQL is a bit special. To start MySQL, use:

```bash
/start-mysql.sh
```

To stop MySQL, use:

```bash
service mysql stop
```
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
