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
 * A pair of a batch type and a row schema for rows within batches of that
 * type. Allows working with batches generically without knowing the actual
 * type of the batch or rows.
 */
public class BatchSchema
{
  private final BatchType batchType;
  private final RowSchema schema;

  public BatchSchema(BatchType batchType, RowSchema schema)
  {
    this.batchType = batchType;
    this.schema = schema;
  }

  public BatchType type()
  {
    return batchType;
  }

  public RowSchema rowSchema()
  {
    return schema;
  }

  public Batch newBatch()
  {
    return batchType.newBatch(schema);
  }

  public Batch of(Object data)
  {
    Batch batch = newBatch();
    batch.bind(data);
    return batch;
  }

  public BatchWriter<?> newWriter(int sizeLimit)
  {
    return batchType.newWriter(schema, sizeLimit);
  }

  public BatchReader newReader()
  {
    return batchType.newReader(schema);
  }
}
