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

package org.apache.druid.queryng.rows;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

import java.util.List;
import java.util.Map;

/**
 * Builds a batch of data from whole rows of data. Primarily for testing.
 * Upon creation, the builder creates a new batch ready for writing. To use
 * the builder for a second batch, call {@link #build()} to get the first batch,
 * then {@link #newBatch()} to start the second.
 */
public class BatchBuilder<T>
{
  private final BatchWriter<T> batch;
  private BatchReader<T> reader;

  public BatchBuilder(final BatchWriter<T> batch)
  {
    this.batch = batch;
    newBatch();
  }

  public static <T> BatchBuilder<T> of(final BatchWriter<T> batch)
  {
    return new BatchBuilder<>(batch);
  }

  public static BatchBuilder<List<Object[]>> arrayList(RowSchema schema)
  {
    return of(new ObjectArrayListWriter(schema));
  }

  public static BatchBuilder<List<Map<String, Object>>> mapList(RowSchema schema)
  {
    return of(new MapListWriter(schema));
  }

  public static BatchBuilder<ScanResultValue> scanResultValue(
      final String datasourceName,
      final RowSchema schema,
      final ScanQuery.ResultFormat format
  )
  {
    return of(new ScanResultValueWriter(datasourceName, schema, format, ScanQuery.DEFAULT_BATCH_SIZE));
  }

  public RowSchema schema()
  {
    return batch.schema();
  }

  public BatchBuilder<T> newBatch()
  {
    batch.newBatch();
    return this;
  }

  public boolean isFull()
  {
    return batch.isFull();
  }

  /**
   * Add a row based on the tuple of values provided as arguments.
    */
  public BatchBuilder<T> row(Object...cols)
  {
    newRow();
    RowWriter writer = batch.row();
    for (int i = 0; i < cols.length; i++) {
      writer.scalar(i).setValue(cols[i]);
    }
    return this;
  }

  private void newRow()
  {
    if (!batch.newRow()) {
      throw new ISE("Batch is full: check isFull() before writing");
    }
  }

  /**
   * Add a row based on a single column. Needed when that one value is an
   * array (and is thus ambiguous for {@link #row(Object...)}, but also
   * works for any column type.
   */
  public BatchBuilder<T> singletonRow(Object col)
  {
    newRow();
    batch.row().scalar(0).setValue(col);
    return this;
  }

  public T build()
  {
    return batch.harvest();
  }

  public BatchReader<T> toReader()
  {
    if (reader == null) {
      reader = batch.toReader();
    } else {
      reader.bind(build());
    }
    return reader;
  }
}
