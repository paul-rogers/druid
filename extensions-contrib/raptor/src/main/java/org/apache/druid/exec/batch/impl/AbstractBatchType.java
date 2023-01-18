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

package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.RowSchema;

public abstract class AbstractBatchType implements BatchType
{
  private final BatchFormat batchFormat;
  private final boolean canSeek;
  private final boolean canSort;
  private final boolean canWrite;

  public AbstractBatchType(
      final BatchFormat format,
      final boolean canSeek,
      final boolean canSort,
      final boolean canWrite
  )
  {
    this.batchFormat = format;
    this.canSeek = canSeek;
    this.canSort = canSort;
    this.canWrite = canWrite;
  }

  @Override
  public boolean canWrite()
  {
    return canWrite;
  }

  @Override
  public boolean canSeek()
  {
    return canSeek;
  }

  @Override
  public boolean canSort()
  {
    return canSort;
  }

  @Override
  public BatchFormat format()
  {
    return batchFormat;
  }

  @Override
  public boolean canDirectCopyFrom(BatchType otherType)
  {
    // The hoped-for default. Revise if the type is more general
    // or restrictive.
    return this == otherType;
  }

  @Override
  public Batch newBatch(RowSchema schema)
  {
    return new BatchImpl(batchSchema(schema));
  }

  @Override
  public BatchSchema batchSchema(RowSchema schema)
  {
    return new BatchSchema(this, schema);
  }
}
