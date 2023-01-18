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
 * A pair of a batch schema and a batch of data having that schema.
 * Allows working with with the data in a type-independent way.
 * <p>
 * Operators pass the underlying data between themselves. A batch
 * is persistent: it typically binds to a series of batches as execution
 * proceeds. This "holder" implementation allows readers to be reused,
 * which allows column readers to be reused, which ensures that setup
 * for projections, filtering, sorting, merging, etc. is done once and
 * cached on the batch.
 */
public interface Batch
{
  BatchSchema schema();
  void bind(Object data);
  BatchCursor newCursor();
  void bindCursor(BatchCursor cursor);
  int size();
  Object data();
}
