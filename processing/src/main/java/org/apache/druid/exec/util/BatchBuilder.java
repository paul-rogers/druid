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

package org.apache.druid.exec.util;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.ColumnWriterFactory;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.shim.MapListWriter;
import org.apache.druid.exec.shim.ObjectArrayListWriter;
import org.apache.druid.exec.shim.ScanResultValueWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.scan.ScanQuery;

/**
 * Builds a batch of data from whole rows of data. Primarily for testing.
 * Upon creation, the builder creates a new batch ready for writing. To use
 * the builder for a second batch, call {@link #build()} to get the first batch,
 * then {@link #newBatch()} to start the second.
 */
public class BatchBuilder
{
  private final BatchWriter batch;

  public BatchBuilder(final BatchWriter batch)
  {
    this.batch = batch;
    newBatch();
  }

  public static BatchBuilder of(final BatchWriter batch)
  {
    return new BatchBuilder(batch);
  }

  public static BatchBuilder arrayList(RowSchema schema)
  {
    return of(new ObjectArrayListWriter(schema));
  }

  public static BatchBuilder mapList(RowSchema schema)
  {
    return of(new MapListWriter(schema));
  }

  public static BatchBuilder scanResultValue(
      final String datasourceName,
      final RowSchema schema,
      final ScanQuery.ResultFormat format
  )
  {
    return of(new ScanResultValueWriter(datasourceName, schema, format, ScanQuery.DEFAULT_BATCH_SIZE));
  }

  public RowSchema schema()
  {
    return batch.columns().schema();
  }

  public BatchBuilder newBatch()
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
  public BatchBuilder row(Object...cols)
  {
    newRow();
    ColumnWriterFactory writer = batch.columns();
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
  public BatchBuilder singletonRow(Object col)
  {
    newRow();
    batch.columns().scalar(0).setValue(col);
    return this;
  }

  public Batch build()
  {
    return batch.harvest();
  }
}
