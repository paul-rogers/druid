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
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchReader.BatchCursor;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;

import java.util.List;

public abstract class AbstractBatchWriter<T> implements BatchWriter<T>
{
  /**
   * Naive copier that simply uses a row writer to project each column
   * one-by-one. Used when the reader/writer pair does not support a
   * more efficient direct copy.
   */
  public class NaiveCopier implements Copier
  {
    private final BatchCursor cursor;
    private final RowWriter rowWriter;

    public NaiveCopier(BatchReader reader)
    {
      // Quick & dirty check on the number of columns. We trust that
      // the caller has ensured the types match or are compatible.
      if (schema().rowSchema().size() != reader.schema().rowSchema().size()) {
        throw new UOE("Cannot copy rows between differing schemas: use a projection");
      }

      this.cursor = reader.batchCursor();
      this.rowWriter = rowWriter(reader.columns().columns());
    }

    @Override
    public int copy(int n)
    {
      int i;
      for (i = 0; i < n; i++) {
        if (!cursor.next()) {
          break;
        }
        if (!rowWriter.write()) {
          // Move back to the prior row since the current one
          // wasn't written.
          cursor.seek(cursor.index() - 1);
          break;
        }
      }
      return i;
    }
  }

  protected final BatchSchema batchSchema;
  protected final int sizeLimit;

  public AbstractBatchWriter(final BatchSchema batchFactory)
  {
    this(batchFactory, Integer.MAX_VALUE);
  }

  public AbstractBatchWriter(final BatchSchema batchFactory, final int sizeLimit)
  {
    this.batchSchema = batchFactory;
    this.sizeLimit = sizeLimit;
  }

  @Override
  public boolean isFull()
  {
    return size() >= sizeLimit;
  }

  @Override
  public Copier copier(BatchReader source)
  {
    return new NaiveCopier(source);
  }

  @Override
  public RowWriter rowWriter(List<ScalarColumnReader> readers)
  {
    if (readers == null || readers.size() != batchSchema.rowSchema().size()) {
      throw new ISE("Reader list count does not match the schema size");
    }
    ScalarColumnReader projections[] = new ScalarColumnReader[readers.size()];
    readers.toArray(projections);
    return newRowWriter(projections);
  }

  protected abstract RowWriter newRowWriter(ScalarColumnReader[] projections);

  @Override
  public Batch harvestAsBatch()
  {
    Batch batch = batchSchema.newBatch();
    batch.bind(harvest());
    return batch;
  }

  @Override
  public BatchSchema schema()
  {
    return batchSchema;
  }
}
