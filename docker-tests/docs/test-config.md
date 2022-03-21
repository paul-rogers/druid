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

## Test Configuration

Tests typically need to understand how the cluster is structured.
To create a test, you must supply at least three key components:

* A `druid-cluster/docker-compose.yaml` file that launches the desired cluster.
  (The folder name `druid-cluster` becomes the application name in Docker.)
* A `src/test/resources/yaml/docker.yaml` file that describes the cluster
  for tests. This file can also include Metastore SQL statements needed to
  populate the metastore.
* The test itself, as a JUnit test that uses the `Initializer` class to
  configure the tests to match the cluster.

This section explains the test configuration file which defines the test
cluster.

## Cluster Types

The integration tests can run in a variety of cluster types, depending
on the details of the test:

* Docker Compose: the normal configuration that all tests support.
* Micro Quickstart: allows for a manual cluster setup, if, say, you
  want to run services in your IDE. Supported by a subset of tests.
* Kubernetes: (Details needed.)

Each cluster type has its own quirks. The job of the tests's cluster configuration
file is to communicate those quirks to the test.

Docker and Kubernetes use proxies to communicate to the cluster. Thus, the host
know to the tests is different than the hosts known within the cluster. Ports
also are mapped differently "outside" than "inside."

Clusters outside of Docker don't provide a good way to start and stop
services, so test that want to do that (to, say, test high availability)
can't run except in a Docker cluser.

### Specify the Cluster Type

To reflect this, test provide named configuration files. The configuration
itself is passed in via the environment:

```bash
export CLUSTER_NAME=quickstart
```

```bash
java ... -Ddruid.test.cluster.name=quickstart
```

The system property taskes precedence over the environment variable.
If neither are set, `docker` is the default.

## Cluster Configuration Files

Cluster configuration is specified in a file for ease of debugging. You
should need no special launch setup in your IDE to run a test if using
the standard Docker Compose cluster for that test.

The configuration file has the same name as the cluster type and resides on
the class path at `/yaml/<type>.yaml` and in the source tree at
`<test module>/src/test/resources/yaml/<type>.yaml`. The standard names are:

* `docker.yaml`: the default and required for all tests. Describes a Docker
  Compose based test.
* `k8s.yaml`: a test cluster running in Kubernetes. (Details needed.)
* `quickstart.yaml`: a Micro Quickstart cluster. (Details needed.)
* `<other>.yaml`: custom cluster configuration.

The result is that you can specify a minimum amount of information in
`cluster.yaml`. Again, see the examples for more information.

## Configuration File Syntax

### `type`

```yaml
type: docker|k8s|local|disabled
```

The type explains the infrastructure that runs the cluster:

* `docker`: a cluster launched in Docker, typically via Docker Compose.
  A proxy host is needed. (See below.)
* `k8s`: a cluster run in Kubernets. (Details needed). A proxy host
  is needed.
* `local`: a cluster running as processes on a network directly reachable
  by the tests. Example: a micro-quickstart cluster running locally.
* `disabled`: the configuration is not supported by the test.

The `disabled` type is handy for tests that require Docker: you can say that
the test is not available when the cluster is local.

If the test tries to load a cluster name that does not exist, a "dummy"
configuration is loaded instead with the type set to `disabled`.

The type is separate from the cluster name (as explained earlier): there
may be multiple names for the same type. For example, you might have two
or three local cluster setups you wish to test.

### `include`

```yaml:
include:
  - <file>
```

Allows including any number of other files. Similar to inheritance for
Docker Compose. The inheritance rules are:

* Properties set later in the list take precedence over properties set in
  files earlier in the list.
* Properties set in the file take precedence over properties set in
  included files.
* Includes can nest to any level.

Merging occurs as follows:

* Top level scalars: newer values replace older values.
* Services: newer values replace all older settings for that service.
* Metastore init: newer values add more queries to any list defined
  by an earlier file.

### `proxyHost`

```yaml
proxyHost: <host name>
```

When tests run in either Docker or Kubernetes, the test communicate with
a proxy, which forwards requests to the cluster hosts and ports. In
Docker, the proxy host is the machine that runs Docker. In Kubernetes,
the proxy host is the host running the Kubernetes proxy service.

There is no proxy host for clusters running directly on a machine.

If the proxy host is omitted for Docker, `localhost` is assumed.

### Service Instance Object

The service sections all allow multiple instances of each service. Service
instances provide a number of properties:

#### `container`

```yaml
container: <container name>
```

Name of the Docker container. If omitted, defaults to:

* `<service name>-<tag>` if a `tag` is provided (see below.)
* The name of the service (if there is only one instance).

#### `host`

```yaml
host: <host name or IP>
```

The host name or IP address on which the instance runs. This is
the host name known to the _cluster_: the name inside a Docker overlay network.
Has teh same defaults as `container`.

#### `port`

```yaml
port: <port>
```

The port number of the service on the container as seen by other
services running within Docker. Required.

(TODO: If TLS is enabled, this is the TLS port.)

#### `proxyPort`

```yaml
proxyPort: <port>
```

The port number for the service as exposed on the proxy host.
Defaults to the same as `port`. You must specify a value if
you run multiple instances of the same service.

### Service Object

Generic object to describe Docker Compose services.

#### `service`

```yaml
service: <service name>
```

Name of the service as known to Docker Compose. Defaults to be
the same as the service name used in this configuration file.

#### `instances`

```yaml
instances:
  - <service-instance>

Describes the instances of the service as `ServiceInstance` objects.
Each service requires at least one instance. If more than one, then
each instance must define a `tag` that is a suffix that distinguishes
the instances.

### `zk`

```yaml
zk:
  <service object>
```

Specifies the ZooKeeper instances.

### `startTimeoutSecs`

```yaml
startTimeoutSecs: <secs>
```

Specifies the amount of time to wait for ZK to become available when using the
test client. Optional.

### `metastore`

```yaml
metastore:
  <service object>
```

Describes the Druid "metadata storage" (metastore) typically
hosted in the offical MySql container. See `MetastoreConfig` for
configuration options.

#### `driver`

```yaml
driver: <full class name>
```

The Driver to use to work with the metastore. The driver must be
available on the tests's class path.

#### `user`

```yaml
user: <user name>
```

The MySQL user name.


#### `password`

```yaml
user: <password>
```

The MySQL password.

#### `properties`

```yaml
properties:
  <key>: <value>
```

Optional map of additional key/value pairs to pass to the JDBC driver.

### `kafka`

```yaml
zk:
  <service object>
```

## `druid`

```yaml
druid:
  <service>:
    <service object>
```

## `metastoreInit`

```yaml
metastoreInit:
  - sql: |
      <sql query>
```

A set of MySQL statements to be run against the
metadata storage before the test starts. Queries run in the
order specified. Ensure each is idempotent to
allow running tests multiple times against the same target directory.

To be kind to readers, please format the statements across multiple lines.
The code will compress out extra spaces before submitting the query so
that JSON payloads are as compact as possible.

The `sql` keyword is the only one supported at present. The idea is that
there may need to be context for some queries in some tests. (To be
enhance as query conversion proceeds.)
