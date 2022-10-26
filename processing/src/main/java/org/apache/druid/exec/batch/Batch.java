/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.exec.batch;

/**
 * Enriched holder of a batch of data for use within each operator.
 * A batch is a generic holder of data, with a factory to work with that
 * data in a type-independent way.
 * <p>
 * Operators may pass the underlying data between themselves. A batch
 * is persistent: it typically binds to a series of batches as execution
 * proceeds. This "holder" implementation allows readers to be reused,
 * which allows column readers to be reused, which ensures that setup
 * for projections, filtering, sorting, merging, etc. is done once and
 * cached on the batch.
 * <p>
 * This pattern works with the incoming data is the same between return
 * values from upstream. If this is not the case, then the batch must
 * be discarded, a new one created with a new schema, and all the setup
 * redone. Fortunately, Druid does not seem to (at present) implement such
 * a "schema change" event.
 */
public interface Batch
{
  BatchFactory factory();
  void bind(Object data);
  BatchReader newReader();
  void bindReader(BatchReader reader);
  int size();
  Object data();
}
