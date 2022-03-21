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

# Test Conversion

To convert an existing test to the new system:

* Create a sub-module here that corresponds to a test group in the
  previous version. Handle the fiddly bits of creating the `pom.xml`
  file and adding the new module to the `pom.xml` file in this module.
* Copy the execution plugin bit from another test. Determine which
  environment variables are needed, and if any can be removed.
* Create a `druid-cluster/docker-compose.yaml` file by converting the
  previous `docker/docker-compose-<group>.yml` file. Carefully review
  each service. Use existing files as a guide.
* Copy and modify the `start-cluster.sh` and `stop-cluster.sh` scripts
  from an existing test. Add lines to copy any test-specific files into
  the `target/shared` folder.
* Create a `test.sh` file that sets the needed environment variables
  (those which Maven will soon provide), and ensure you can launch your
  cluster.
* Create a new `src/test/java` directory.
* Copy the existing tests for the target group into the new directory.
  For sanity, you may want to do one by one.
* Create the `src/test/resources/cluster.yaml` file with the cluster
  description and any needed metadata setup. (See the former `druid.sh`
  script to see what SQL was used previously.)
* Create a scafollding test file, say `StarterTest` to hold one of the
  tests to be converted. Copy any needed Guice injections. This will
  be a JUnit test.
* Add the initialization code by grabbing it from an existing test.
  You need both the `@Before` code and the constructor.
* Run the one test. This will find bugs in the above. Resolve them.
* One-by-one, convert existing tests:
  * Remove the TestNG annotations and includes. Substitute JUnit includes.
  * Add the intitialization code as in the scaffolding test.
  * Run the test in the debugger to ensure it works.
* Run the entire suite from Maven in the sub-module directory. It should
  start the cluster, run the tests, and shut down the cluster.
