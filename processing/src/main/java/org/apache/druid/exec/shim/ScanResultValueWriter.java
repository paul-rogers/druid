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

package org.apache.druid.exec.shim;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

import java.util.Collections;
import java.util.List;

public class ScanResultValueWriter extends DelegatingBatchWriter
{
  private final String segmentId;
  private final ScanQuery.ResultFormat format;
  private final List<String> columnNames;

  public ScanResultValueWriter(
      final String segmentId,
      final RowSchema schema,
      final ScanQuery.ResultFormat format,
      final int sizeLimit
  )
  {
    super(createDelegate(schema, format, sizeLimit));
    this.segmentId = segmentId;
    this.format = format;
    this.columnNames = schema.columnNames();
  }

  private static BatchWriter createDelegate(final RowSchema schema, final ScanQuery.ResultFormat format, final int sizeLimit)
  {
    switch (format) {
      case RESULT_FORMAT_LIST:
        return new MapListWriter(schema, sizeLimit);
      case RESULT_FORMAT_COMPACTED_LIST:
        return new ObjectArrayListWriter(schema, sizeLimit);
      default:
        throw new UOE(format.name());
    }
  }

  @Override
  public Batch harvest()
  {
    final List<?> result;
    switch (format) {
      case RESULT_FORMAT_LIST:
      case RESULT_FORMAT_COMPACTED_LIST:
        result = ((ListWriter<?>) delegate).harvestList();
        break;
      default:
        result = Collections.emptyList();
    }
    return new ScanResultValueBatch(
        columns().schema(),
        format,
        new ScanResultValue(segmentId, columnNames, result)
    );
  }

  @Override
  public boolean canDirectCopyFrom(BatchReader reader)
  {
    if (reader instanceof ScanResultValueReader) {
      ScanResultValueReader source = (ScanResultValueReader) reader;
      return source.format() == format;
    } else {
      return delegate.canDirectCopyFrom(reader);
    }
  }

  @Override
  public int directCopy(BatchReader from, int n)
  {
    if (from instanceof ScanResultValueReader) {
      ScanResultValueReader source = (ScanResultValueReader) from;
      from = source.delegate();
    }
    return delegate.directCopy(from, n);
  }
}
