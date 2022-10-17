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

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;

import java.util.List;
import java.util.Map;

/**
 * Batch facade over a {@code ScanQuery} {@code ScanResultValue}.
 */
public class ScanResultValueReader extends DelegatingBatchReader<ScanResultValue>
{
  private ListReader<?> delegate;

  public ScanResultValueReader(final RowSchema schema, ScanQuery.ResultFormat format)
  {
    this.delegate = createDelegate(schema, format);
  }

  private static ListReader<?> createDelegate(RowSchema schema, ResultFormat format)
  {
    if (format == null) {
      // We don't know what this is, but we also have no rows. Special case.
      return Batches.emptyListReader(schema);
    }
    switch (format) {
      case RESULT_FORMAT_LIST:
        return new MapListReader(schema);
      case RESULT_FORMAT_COMPACTED_LIST:
        return new ObjectArrayListReader(schema);
      default:
        throw new UOE(format.name());
    }
  }

  public static ScanResultValueReader of(ScanResultValue batch)
  {
    ScanQuery.ResultFormat format = inferFormat(batch);
    ScanResultValueReader batchReader = new ScanResultValueReader(inferSchema(batch, format), format);
    batchReader.bind(batch);
    return batchReader;
  }

  private static ResultFormat inferFormat(ScanResultValue batch)
  {
    if (batch.getRows().isEmpty()) {
      return null;
    }
    if (batch.getRows().get(0) instanceof Map) {
      return ScanQuery.ResultFormat.RESULT_FORMAT_LIST;
    }
    return ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST;
  }

  private static RowSchema inferSchema(ScanResultValue batch, ScanQuery.ResultFormat format)
  {
    List<String> columnNames = batch.getColumns();
    if (format == null) {
      return TypeInference.untypedSchema(columnNames);
    }
    switch (format) {
      case RESULT_FORMAT_LIST:
        return TypeInference.inferMapSchema(batch.getRows(), columnNames);
      case RESULT_FORMAT_COMPACTED_LIST:
        return TypeInference.inferArraySchema(batch.getRows(), columnNames);
      default:
        throw new UOE("Result format not supported");
    }
  }

  @Override
  protected BatchReader<?> delegate()
  {
    return delegate;
  }

  @Override
  public void bind(ScanResultValue batch)
  {
    delegate.bind(batch.getRows());
  }
}
