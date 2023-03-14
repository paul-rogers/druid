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

# gRPC Query Extension for Druid

This extension provides a gRPC API for SQL queries.

Druid uses REST as its RPC protocol. Druid has a large variety of REST operations
including query, ingest jobs, monitoring, configuration and many more. Although
REST is a universally supported RPC format, is not the only one in use. This
extension allows gRPC-based clients to issue SQL queries.

Druid is optimized for high-concurrency, low-complexity queries that return a
small result set (a few thousand rows at most). The small-query focus allows
Druid to offer a simple, stateless request/response REST API. This gRPC API
follows that Druid pattern: it is optimized for simple queries and follows
Druid's request/response model. APIs such as JDBC can handle larger results
because they stateful: a client can request pages of results using multiple
API calls. This API does not support paging: the entire result set is returned
in the response, resulting in a API which is fast for small queries, and not
suitable for larger result sets.

## Use Cases

The gRPC query extension can be used in two ways, depending on the selected
result format.

### CSV or JSON Response Format

The simplest way to use the gRPC extension is to send a query request that
uses CSV or JSON as the return format. The client simply pulls the results
from the response and does something useful with them. For the CSV format,
headers can be created from the column metadata in the response message.

### Protobuf Response Format

Some applications want to use Protobuf as the result format. In this case,
the extension encodes Protobuf-encoded rows as the binary payload of the query
response. This works for an application which uses a fixed set of queries, each
of which is carefully designed to power one application, say a dashboard. The
(simplified) message flow is:

```text
+-----------+   query ->  +-------+
| Dashboard | -- gRPC --> | Druid |
+-----------+  <- data    +-------+
```

In practice, there may be multiple proxy layers: one on the application side, and
the Router on the Druid side.

The dashboard displays a fixed set of reports and charts. Each of those sends a
well-defined query specified as part of the application. The returned data is thus
both well-known and fixed for each query. The set of queries is fixed by the contents
of the dashboard. That is, this is not an ad-hoc query use case.

Because the queries are locked down, and are part of the application, the set of valid
result sets is also well known and locked down. Given this well-controlled use case, it
is possible to use a pre-defined Protobuf message to represent the results of each distinct
query. (Protobuf is a compiled format: the solution works only because the set of messages
are well known. It would not work for the ad-hoc case in which each query has a different
result set schema.)

To be very clear: the application has a fixed set of queries to be sent to Druid via gRPC.
For each query, there is a fixed Protobuf response format defined by the application.
No other queries, aside from this well-known set, will be sent to the gRPC endpoint using
the Protobuf response format. If the set of queries is not well-defined, use the CSV
or JSON response format instead.

## Installation

The gRPC query extension is a "contrib" extension and is not installed by default when
you install Druid. Instead, you must install it manually.

In development, you can build Druid with all the "contrib" extensions. When building
Druid, include the `-P bundle-contrib-exts` in addition to the `-P dist` option:

```bash
mvn package -Pdist,bundle-contrib-exts ...
```

In production, follow the [Druid documentation](https://druid.apache.org/docs/latest/development/extensions.html).
Do not use the `pull-deps` command to install the `org.apache.druid.extensions.contrib:grpc-query`
extension. Instead, unpack the `.tar.gz` file created in the `grpc-query` project.

This extension provides a simpler alternative: it products a bundle named
`grpc-query-<version>.tar.gz` with the `grpc-query` jar and its required
dependencies, which include only a shaded rRPC jar file. All other dependencies
are provided by Druid itself. To install, simply unpack the `tar.gz` file into
the `extensions` folder:

```bash
cd $DRUID_HOME/extensions
tar -xzf /path/to/grpc-query-<version>.tar.gz
```

To enable the extension, add the following to the load list in
`_commmon/common.runtime.properties`:

```text
druid.extensions.loadList=[..., "grpc-query"]
```

Adding the extension to the load list automatically enables the extension,
but only in the Broker.

If you use the Protobuf response format, bundle up your Protobuf classes
into a jar file, and place that jar file in the
`$DRUID_HOME/extensions/grpc-query` directory. The Protobuf classes will
appear on the class path and will be available from the `grpc-query`
extension.

### Configuration (Prototype)

Configure the extension in `broker/runtime.properties`:

```text
druid.grpcQuery.port=50051
```

The default port is 50051 (preliminary).

### Configuration (Production - Preliminary)

Enable HTTP/2 support on either plain text or TLS ports for the Broker
in `broker/runtime.properties`:

```text
druid.server.http.enableHttp2=true
druid.server.https.enableHttp2=true
```

With this approach, the gRPC query endpoint is available on the configured
ports (usually 8082 for plain text, and 8182 for TLS.)

## Usage

See the `src/main/proto/query.proto` file in the `grpc-shaded` project for the request and
response message formats. The request message format closely follows the REST JSON message
format. The response is optimized for gRPC: it contains an error (if the request fails),
or the result schema and result data as a binary payload. You can query the gRPC endpoint
with any gRPC client.

Although both Druid SQL and Druid itself support a `float` data type, that type is not
usable in a Protobuf response object. Internally Druid converts all `float` values to
`double`. As a result, the Protobuf reponse object supports only the `double` type.
An attempt to use `float` will lead to a runtime error when processing the query.
Use the `double` type instead.

## Implementation Notes

The extension is made up of three projects. Druid uses a different version of Guava than
does rRPC. To work around this, the `grpc-shade` project creates a shaded jar that includes
both gRPC and Guava. Since the rRPC compiler generates references to Guava, it turns out we
must generate Java from the `.proto` files _before_ shading, so the `.proto` files, and
the generated Java code, also reside in the `grpc-shade` project. One unfortunate aspect
of shading is that your IDE will not be able to find the source attachments for the rRPC
classes. The result is very tedious debugging. A workaround is to manually attach the
original gRPC source jars for each gRPC source file.

This project contains several components:

* Guice modules and associated server initialization code.
* Netty-based gRPC server.
* A "driver" that performs the actual query and generates the results.

The `grpc-query-it` project provides integration tests for the extension. These tests
run using the "new" IT framework as described in the `integration-tests-ex/doc/README.md`
file.

## Debugging

Debugging of the gRPC extension requires extra care due to the nuances of loading
classes from an extension.

### Running in a Server

Druid extensions are designed to run in the Druid server. The gRPC extension is
loaded only in the Druid broker using the contiguration described above. If something
fails during startup, the Broker will crash. Consult the Broker logs to determine
what went wrong. Startup failures are typically due to required jars not being installed
as part of the extension. Check the `pom.xml` file to track down what's missing.

Failures can also occur when running a query. Such failures will result in a failure
response and should result in a log entry in the Broker log file. Use the log entry
to sort out what went wrong.

You can also attach a debugger to the running process. You'll have to enable the debugger
in the server by adding the required parameters to the Broker's `jvm.config` file.

### Debugging using Unit Tests

To debug the functionality of the extension, your best bet is to debug in the context
of a unit test. Druid provides a special test-only SQL stack with a few pre-defined
datasources. See the various `CalciteQueryTest` classes to see what these are. You can
also query Druid's various system tables. See `GrpcQueryTest` for a simple "starter"
unit test that configures the server and uses an in-process client to send requests.

Most unit testing can be done without the gRPC server, by calling the `QueryDriver`
class directly. That is, if the goal is work with the code that takes a request, runs
a query, and produces a response, then the driver is the key and the server is just a
bit of extra copmlexity. See the `DriverTest` class for an example unit test.

### Debugging in a Server in an IDE

We would like to be able to debug the gRPC extension, within the Broker, in an IDE.
As it turns out, doing so breaks Druid's class loader mechanisms in ways that are both
hard to understand and hard to work around. When run in a server, Java creates an instance
of `GrpcQueryModule` using the extension's class loader. Java then uses that same class
loader to load other classes in the extension, including those here and those in the
shaded gRPC jar file.

However, when run in an IDE, if this project is on the class path, then the `GrpcQueryModule`
class will be loaded from the "App" class loader. This works fine: it causes the other
classes of this module to also be loaded from the class path. However, once execution
calls into gRPC, Java will use the App class loader, not the extension class loader, and
will fail to find some of the classes, resulting in Java exceptions. Worse, in some cases,
Java may load the same class from both class loaders. To Java, these are not the same
classes, and you will get mysterious errors as a result.

For now, the lesson is: don't try to debug the extension in the Broker in the IDE. Use
one of the above options instead.

For reference (and in case we figure out a solution to the class loader conflict),
the way to debug the Broker in an IDE is the following:

* Build your branch. Use the `-P bundle-contrib-exts` flag in place of `-P dist`, as described
  above.
* Create an install from the distribution produced above.
* Use the `single-server/micro-quickstart` config for debugging.
* Configure the installation using the steps above.
* Modify the Supervisor config for your config to comment out the line that launches
  the broker. Use the hash (`#`) character to comment out the line.
* In your IDE, define a launch configuration for the Broker.
  * The launch command is `server broker`
  * Add the following JVM arguments:

```text
--add-exports java.base/jdk.internal.perf=ALL-UNNAMED
--add-exports jdk.management/com.sun.management.internal=ALL-UNNAMED
```

  * Define `grpc-query` as a project dependency. (This is for Eclipse; IntelliJ may differ.)
  * Configure the class path to include the common and Broker properties files.
  * Explicitly add the `extension/grpc-query/druid-shaded-grpc-<version>-SNAPSHOT.jar` in
    the class path.
* Launch the micro-quickstart cluster.
* Launch the Broker in your IDE.

### Debugging using Integration Tests

Use integration tests to verify the full behavior of the extension, including resolution
of Protobuf classes, and integration with Druid. To run the integration test:

```bash
./it.sh build
./it.sh image
./it.sh test GrpcQuery extensions-contrib/grpc-query-it
```

To debug an integration test:

```bash
./it.sh build
./it.sh image
./it.sh up GrpcQuery extensions-contrib/grpc-query-it
```

Then, run the `GrpcQueryTest` class in `grpc-query-it` in your IDE. When
done debugging:

```bash
./it.sh build
./it.sh image
./it.sh up GrpcQuery extensions-contrib/grpc-query-it
```

Since `grpc-query` is an extension. To run tests after changes, you need only build
that one module:

```bash
mvn clean package -P dist,skip-static-checks,skip-tests -Dmaven.javadoc.skip=true \
    -Dcyclonedx.skip=true -pl :grpc-query
```

Then, restart the cluster (`down`, then `up`). The `up` (or `test`) commands
populate the cluster with your newly rebuilt `grpc-query` extension and test
Protobuf classes.

### gRPC Logging

Debugging of the gRPC stack is difficult since the shaded jar loses source attachments.

Logging helps. gRPC logging is not enabled via Druid's logging system. Intead, [create
the following `logging.properties` file](https://stackoverflow.com/questions/50243717/grpc-logger-level):

```text
handlers=java.util.logging.ConsoleHandler
io.grpc.level=FINE
java.util.logging.ConsoleHandler.level=FINE
java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter
```

Then, pass the following on the command line:

```text
-Djava.util.logging.config.file=logging.properties
```

Adjust the path to the file depending on where you put the file.

## Acknowledgements

This is not the first project to have created a gRPC API for Druid. Others include:

* [[Proposal] define a RPC protocol for querying data, support apache Arrow as data
  exchange interface](https://github.com/apache/druid/issues/3891)
* [gRPC Druid extension PoC](https://github.com/ndolgov/gruid)
* [Druid gRPC-json server extension](https://github.com/apache/druid/pull/6798)

Full credit goes to those who have gone this way before.

Note that the class loader solution used by the two code bases above turned out
to not be needed. See the notes above about the class loader issues.
