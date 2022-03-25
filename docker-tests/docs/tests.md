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

# Test Structure

The structure of integration tests is heavily influenced by the existing
test structure. In that previous structure:

* Each test group ran as separate Maven build.
* Each would build an image, start a cluster, run the test, and shut down the cluster.
* Tests were created using [TestNG](https://testng.org/doc/), a long-obsolete
  test framework.
* A `IntegrationTestingConfig` is created from system properties (passed in from
  Maven via `-D<key>=<value>` options).
* A TestNG test runner uses a part of the Druid Guice configuration to inject
  test objects into the tests.
* The test then runs.

To minimize test changes, we try to keep much of the "interface" while changing
the "implementation". Basically:

* The same Docker image is used for all tests.
* Each test defines its own test cluster.
* Maven runs tests one by one, starting and stopping the cluster for each.
* A `CluserConfig` object defines the test configuration and creates the
  `IntegrationTestingConfig` object.
* An instance of `Initializer` sets up Guice for each test and injects the
  test objects.
* Tests run as JUnit tests.

The remainer of this section describes the test internals.

## Test Configuration

See [Test Configuration](test-config.md) for details on the `docker.yaml` file
that you create for each test module to tell the tests about the cluster you
have defined.

Test configuration allows inheritance so, as in Docker Compose, we define
standard bits in one place, just providing test-specific information in each
tests `docker.yaml` file.

The test code assumes that the test configuration file is in `src/test/resources/yaml/docker.yaml`
(or, specifically that it is on the class path at `/yaml/docker.yaml`)
and loads it automatically into a `ClusterConfig` instance.

The `ClusterConfig` instance provides the backward-compatible
`IntegrationTestingConfig` instance.

New tests may want to work with `CluserConfig` directly as the older interface
is a bit of a muddle in several areas.

## Initialization

We want the new JUnit form of the integration tests to be as simple as possible
to debug. Rather than use a JUnit test suite, we instad make each test class
independent. To do this, we insert code that initializes Guice from the test
configuration:

```java
public class MyTest
{
  private static Initializer initializer;

  @Inject
  private SomeObject myObject;
  ...

  public MyTest()
  {
    Initializer
      .builder()
      .test(this)
      .validateCluster()
      .build();
  }
```

Here's what happens:

* JUnit calls the constructor once per test class.
* The `Initializer` loads the configuration and creates the Guice injector.
* The `validateCluster()` causes initialization to check the health of
  each service prior to starting the tests.
* The constructor uses the staic initializer to inject dependencies into itself.
* The test is now configured just as it would be from TestNG, and is ready to run.

The initializer loads the basic set of Druid modules to run the basic client
code. Tests may wish to load additional modules specific to that test. To
do that, pass the list of such modules to the `modules()` method on the
builder.

## `ClusterConfig`

The `ClusterConfig` class is the Java representation of the
[test configuration](test-config.md). The instance is available from the
`Initializer` and by Guice injection.

It is a Jackson-serialized class, in this
case, from YAML. There are two sets of methods: `foo()` and `resolveFoo()`.
You don't want the `foo()` methods unless you are Jackson. You want the
`resolveFoo()` methods which use the various inheriance rules to work
out a value.

Remember that each host has two names and two ports:

* The external (or "proxy") host and port, as seen by the machine running
  the tests.
* The internal host and port, as seen by the service itself running
  in the Docker cluster.

The various [config files](test-config.md) provide configurations for
the Docker, K8s and local cluster cases. This means that `resolveProxyHost()`
will resolve to the proxy for Docker, but the acutal host for a local cluster.

## `ClusterClient`

The integration tests make many REST calls to the Druid cluster. The tests
contain much copy/paste code to make these calls. The `ClusterClient` class
is intended to gather up these calls so we have a single implementation
rather than many copies. Add methods as needed for additional APIs.

The cluster client is "test aware": it uses the information in
`ClusterConfig` to know how to send the requested API. The methods handle
JSON deserialization, so tests can focus simply on making a call and
checking the results.

