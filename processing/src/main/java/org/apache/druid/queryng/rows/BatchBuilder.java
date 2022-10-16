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

import org.apache.druid.queryng.rows.Batch.BatchWriter;

import java.util.List;
import java.util.Map;

/**
 * Builds a batch of data from whole rows of data. Primarily for testing.
 */
public class BatchBuilder<T>
{
  private final BatchWriter<T> batch;

  public BatchBuilder(final BatchWriter<T> batch)
  {
    this.batch = batch;
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

  /**
   * Add a row based on the tuple of values provided as arguments.
    */
  public BatchBuilder<T> row(Object...cols)
  {
    batch.writer().next();
    for (int i = 0; i < cols.length; i++) {
      batch.writer().scalar(i).setValue(cols[i]);
    }
    return this;
  }

  /**
   * Add a row based on a single column. Needed when that one value is an
   * array (and is thus ambiguous for {@link #row(Object...)}, but also
   * works for any column type.
   */
  public BatchBuilder<T> singletonRow(Object col)
  {
    batch.writer().next();
    batch.writer().scalar(0).setValue(col);
    return this;
  }

  public T build()
  {
    return batch.harvest();
  }
}
