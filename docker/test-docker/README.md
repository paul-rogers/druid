# Druid Test Docker Image

This project builds the Docker image for Druid (only) for integration tests.
The Docker compose files use "official" images for dependencies.

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

### Added Later

The above provides the Druid software. To run we use `docker-compose` to
fill in the operational bits:

* Data directories
* Log directory
* Configuration files

These are provided via a "shared" directory mounted into the container.
The shared directory is build in the `target` folder for each test.

------

Old version

This project builds the Docker image for integration test as the second part of
a two-part process. See ``../base-image/README.md` for information on the first part.

This stage adds artifacts from the build process including the Druid distribution
tarball itself.


## Manual Test Runs

The build process is optimized for development speed. While you can run a full build,
doing so is tedious and wasteful. Instead, do the following.

### Build the Base Image (Once)

* Build the base image. Do this once, then only when external dependencies change.

```bash
cd $DRUID_DEV/docker/base-image
mvn -P base-image install
```

### On Each Druid Build

* Do a distribution build of Druid:

```bash
cd $DRUID_DEV
mvn clean package -P dist,skip-static-checks,skip-tests -Dmaven.javadoc.skip=true -T1.0C
```

* Build the test image. Do this each time you build Druid as above.

```bash
cd $DRUID_DEV/docker/test-image
mvn -P test-image install
```

The above step is a bit tedious: you have to wait for `mvn` to grind through things you
don't care about. To go faster, use:

```bash
cd $DRUID_DEV/docker/test-image
./build-image.sh <version>
```

Remember to specify the current Maven version. (All Maven does for us is to invoke this
script with the current version, which is why we can do the task by hand faster.)

