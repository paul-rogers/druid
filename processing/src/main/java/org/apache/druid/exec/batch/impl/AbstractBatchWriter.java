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
import org.apache.druid.exec.batch.BatchFactory;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.ColumnWriterFactory;
import org.apache.druid.java.util.common.UOE;

public abstract class AbstractBatchWriter<T> implements BatchWriter<T>
{
  protected final BatchFactory batchFactory;
  protected final int sizeLimit;
  protected ColumnWriterFactory columnWriters;

  public AbstractBatchWriter(final BatchFactory batchFactory)
  {
    this(batchFactory, Integer.MAX_VALUE);
  }

  public AbstractBatchWriter(final BatchFactory batchFactory, final int sizeLimit)
  {
    this.batchFactory = batchFactory;
    this.sizeLimit = sizeLimit;
  }

  @Override
  public ColumnWriterFactory columns()
  {
    return columnWriters;
  }

  @Override
  public boolean isFull()
  {
    return size() >= sizeLimit;
  }

  @Override
  public boolean newRow()
  {
    if (isFull()) {
      return false;
    }
    createRow();
    return true;
  }

  protected abstract void createRow();

  @Override
  public int directCopy(BatchReader from, int n)
  {
    throw new UOE(
        "Cannot perform a direct copy [%s] -> [%s]. See Batches.copier()",
        from.getClass().getSimpleName(),
        getClass().getSimpleName()
    );
  }

  @Override
  public Batch harvestAsBatch()
  {
    Batch batch = batchFactory.newBatch();
    batch.bind(harvest());
    return batch;
  }

  @Override
  public BatchFactory factory()
  {
    return batchFactory;
  }
}
