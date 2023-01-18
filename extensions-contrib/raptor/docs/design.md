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

# Internal Design

## Fragment Runner

* Represents a unit of execution (between exchanges, like an MSQ frame processor)
* Has a DAG: one root, perhaps multiple leaves
* Has a context
* Has a runner that handles startup & cleanup, etc.

### Fragment Builder

* Takes a physical plan
* Builds a fragment, including operators

## Batch Format

### Frame-base Approach

* Start with frames.
* Frame writer needs
  * A row schema as a Rowsignature
  * A list of sort columns
  * A column selector factory for values
  * An allocator, which needs a capacity
* Frame writer must write to memory: can't reuse the existing memory

To get these:

* Convert from internal for format to RowSignature
* Wrap internal column writers in column selectors & a factory
* Pass sort columns only when needed.
* Wrap the whole thing in a batch writer
* Add to the fragment context the memory allocator stuff.

* Frame reader needs
  * A row signature
  * A frame
* Produces a CursorFactory
* Which has `makeCursors()` (which produces a single cursor? More?)
* MakeCursors needs a filter, an interval, virtual columns, granularity, metrics. (Too much!)
* Data is then read via a cursor

To get these:

* Row signature as above
* Frame as incoming batch
* Wrap the cursor in a batch reader
* Wrap the column selectors in the internal flavor

### Bespoke Batch Approach

* New frame format
* Java object with memory buffer
* Row count
* Row offset array
* Buffer
  * Fixed-width for scalars
  * Length + bytes for string, etc.
  * Length + (field contents as above) for arrays
* Buffer is just the data
* Writer populates the buffer
* Reader reads from the buffer
* Uses new-style readers, writers
* Materialized to buffer only for exchanges
