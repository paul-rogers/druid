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

# Future Work

The present version is very much a work in progress. Work completed to
date includes:

* Restructure the Docker images to use the Druid produced from the
  Maven build. Use "official" images for dependencies.
* Restructure the Docker compose files.
* Create the cluster configuration mechanisms.
* Convert one "test group" to a sub-module: "high-availability".
* Create the `pom.xml`, scripts and other knick-knacks needed to tie
  everything together.
* Create the initial test without using security settings to aid
  debugging.

However, *much* work remains:

* Convert remaining tests.
* Decide when we need full security. Convert the many certificate
  setup scripts.
* Support cluster types other than Docker.

## Later Tasks

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
