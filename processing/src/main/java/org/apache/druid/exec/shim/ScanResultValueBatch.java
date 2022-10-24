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

import org.apache.druid.exec.batch.impl.BatchCapabilitiesImpl;
import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchCapabilities.BatchFormat;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.util.TypeInference;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;

import java.util.List;
import java.util.Map;

/**
 * Batch which represents a {@link ScanResultValue} produced by the {@code ScanQuery}.
 * The value has a name-only schema and rows in either object-array or map format.
 */
public class ScanResultValueBatch implements Batch
{
  public static final BatchCapabilities LIST_CAPABILITIES = new BatchCapabilitiesImpl(
      BatchFormat.SCAN_MAP,
      true, // Can seek
      false // Can't sort
  );
  public static final BatchCapabilities COMPACT_LIST_CAPABILITIES = new BatchCapabilitiesImpl(
      BatchFormat.SCAN_OBJECT_ARRAY,
      true, // Can seek
      false // Can't sort
  );

  private final RowSchema schema;
  private final ScanQuery.ResultFormat format;
  private final ScanResultValue batch;

  public ScanResultValueBatch(
      final RowSchema schema,
      final ScanQuery.ResultFormat format,
      final ScanResultValue batch
  )
  {
    this.schema = schema;
    this.format = format;
    this.batch = batch;
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }

  @Override
  public int size()
  {
    return batch.rowCount();
  }

  @Override
  public BatchCapabilities capabilities()
  {
    switch (format) {
      case RESULT_FORMAT_LIST:
        return LIST_CAPABILITIES;
      case RESULT_FORMAT_COMPACTED_LIST:
        return COMPACT_LIST_CAPABILITIES;
      default:
        throw new UOE("Unsupported format [%s]", format);
    }
  }

  @Override
  public BatchReader newReader()
  {
    final ScanResultValueReader reader = new ScanResultValueReader(schema, format);
    reader.bind(batch);
    return reader;
  }

  @Override
  public BatchReader bindReader(BatchReader reader)
  {
    if (reader == null || !(reader instanceof ScanResultValueReader)) {
      return newReader();
    }
    ((ScanResultValueReader) reader).bind(batch);
    return reader;
  }

  public static ScanResultValueBatch of(ScanResultValue batch)
  {
    final ScanQuery.ResultFormat format = inferFormat(batch);
    return new ScanResultValueBatch(inferSchema(batch, format), format, batch);
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
    final List<String> columnNames = batch.getColumns();
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
  public BatchWriter newWriter()
  {
    // TODO: Get the batch size from somewhere.
    return new ScanResultValueWriter(null, schema, format, ScanQuery.DEFAULT_BATCH_SIZE);
  }
}
