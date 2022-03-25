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

# Maven Structure

The integration tests are built and run as part of Druid's Maven script.
The tests run on the Druid build, and so come *after* the `distribution`
project which builds the Druid tarball. Thus, when you run Maven, we
first build the Druid modules, then assemble them into the tarball, then
build the Docker test image based on that tarball, and finally run the
integration tests using the test image. The result is that a single pass
through Maven will build Druid and run the tests.

## Projects

The key projects in the above flow include:

* `distribution`: Builds the Druid tarball which the ITs exercise.

Then, this module is a collection of sub-modules:

* `testing-tools`: Testing extensions added to the Docker image.
* `test-image`: Builds the Druid test Docker image.
* `base-test`: Test code used by all tests.
* `<group>`: The remaining projects represent test groups: sets of
  tests which use the same Cluster configuration. A new group is created
  when test need a novel cluster configuration.

## `org.apache.druid.testing2`

The `org.apache.druid.testing2` is temporary: it holds code from the
`integration-tests` `org.apache.druid.testing` package adapted to work
in the revised environment. Some classes have the same name in both
places. The goal is to merge the `testing2` package back into
`testing` at some future point when the tests are all upgraded.

## Profiles

Integration test artifacts are build only if you specifically request them
using a profile.

* `-P test-image` builds the test Docker image.
* `-P docker-tests` runs the integration tests.

The two profiles allow you to build the test image once during debugging,
and reuse it across multiple test runs. (See [Debugging](debugging.md).)

When run in Travis, the tests will run one after another in Maven project
order.

## Dependencies

The Docker image inclues three third-party dependencies not included in the
Druid build:

* MySQL connector
* MariaDB connector
* Kafka Protobuf provider

We use dependency rules in the `test-image/pom.xml` file to cause Maven to download
these dependencies into the Maven cache, then we use the
`maven-dependency-plugin` to copy those dependencies into a Docker directory,
and we use Docker to copy the files into the image. This approach avoids the need
to pull the dependency from a remote repository into the image directly, and thus
both speeds up the build, and is kinder to the upstream repositories.

If you add additional dependencies, please follow the above process. See the
`pom.xml` files for examples.

## Environment Variables

Maven communicates with Docker and Docker Compose via environment variables
set in the `exec-maven-plugin` of various `pom.xml` files. The environment
variables then flow into either the Docker build script (`Dockerfile`) or the
various Docker Compose scripts (`docker-compose.yaml`). It can be tedious to follow
this flow. A quick outline:

* Maven, via `exec-maven-plugin`, sets environment variables typically from Maven's
  own variables for things like versions.
* `exec-maven-plugin` invokes a script to do the needes shell fiddling. The environment
  variables are visible to the script and implicitly passed to whatever the script
  calls.
* When building, the script passes the environment variables to Docker as build
  arguments.
* `Dockerfile` typically passes the build arguments to the image as environment
  variables as the same name.
* When running, teh script passes environment variables implicitly to Docker Compose,
  which uses them in the various service configurations and/or environment variable
  settings passed to each container.

If you find you need a new parameter in either the Docker build, or the Docker Compose
configuration:

* First ensure that there is a Maven setting that holds the desired parameter.
* Wire it through the relevant `exec-maven-plugin` sections.
* Pass it to Docker or Docker Compose as above.

The easiest way to test is to insert (or enable, or view) the environment in the
image:

```shell
env
```

The output will typically go into the Docker output or the Docker Compose logs.

## Shortcuts

You can build just the docker image. Assuming `DRUID_DEV` points to your development
area:

```bash
cd $DRUID_DEV
mvn install -P test-image
```

Here "install" means to install the image in the local Docker image repository.

Druid's Maven configuration has many plugins which Maven must grind through on each
build if you are impatient, you can actually remove Maven from the build path for
the image and tests by using a script to do what `exec-maven-plugin` does. You just
have to be sure to set the needed environment variables to the proper values.
Once you do that, creating an image, or launching a cluster, is quite fast.
