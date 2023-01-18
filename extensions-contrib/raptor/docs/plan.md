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

## Goal

* DAG-based execution engine
* Reuses existing mechanisms
* Wrapped in a native query
* Can run on its own

## Constraints

* Previous hybrid path didn't fly: go for a pure DAG engine
* Reuse the batch logic. (Reconcile with Eric's stuff later.)
* Reuse the "query NG" operators (with work)
* Eventually use frames

## Path

* Create an extension to hold the engine: "Raptor".
* Start with metadata queries
* Define a reader format for Java data
* Filter, project, sort, operators
* Hand-written native query against unit tests data
* Calcite logical-to-physical generator
* Data buffer format (or adapt frames)

Then scan query:

* Merge
* Scatter/gather
* Cursor API

Then group-by, etc.

## Concerns

* Async: test framework for high-concurrency queries with fixed thread count
* Memory-efficient merge buffers

## Steps

Getting started:

* Create new branch from master (Done)
* Create the extension (Done)
* Pull in framework from window branch (but not window Fns) (Done)
* Pull in basic operators from prior branch (Done)

Beach head:

* Preferred batch format.
* Fragment, operator structure (Reuse from `exec`).
* Data gen operator (Reuse from `exec` for now, replace later)
* Filter operator
* Project operator (w/out expressions)

Frames

* Simple unit test to write, then read a frame.
* Wrap in batch methods.
* Add to the batch read/write tests.

Expand:

* Create the Java reader
* Test some basic queries.
* Sort out the concurrent query approach & test
* Work out an efficient sort algorithm using buffers of the form to be used elsewhere
* Wire up to the native query framework

Segments

* Isolate the segment lookup from the query execution.
* Operator to do the segment lookup, retry logic

## Build Up the Query Stack

* Fragment framework
  * Fragment, operator, profile, DAG builder, etc.
* Batch format
* Result test framework
* Data gen operator
* Java reader
* Filter operator
* Project (remove columns only)
* Project (expressions)
* Unordered merge
* Sorter
* Ordered merge
* Merge join
