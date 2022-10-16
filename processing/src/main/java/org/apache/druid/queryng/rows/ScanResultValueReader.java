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
import org.apache.druid.queryng.rows.Batch.AbstractBatch;
import org.apache.druid.queryng.rows.Batch.BatchReader;
import org.apache.druid.segment.column.ColumnType;

import java.util.List;
import java.util.Map;

/**
 * Batch facade over a {@code ScanQuery} {@code ScanResultValue}.
 */
public class ScanResultValueReader extends AbstractBatch implements BatchReader<ScanResultValue>
{
  private final RowReader reader;
  private ScanResultValue batch;

  public ScanResultValueReader(final RowSchema schema, ScanQuery.ResultFormat format)
  {
    super(schema);
    this.reader = createReader(format);
  }

  public static ScanResultValueReader of(ScanResultValue batch)
  {
    ScanQuery.ResultFormat format = inferFormat(batch);
    ScanResultValueReader batchReader = new ScanResultValueReader(inferSchema(batch, format), format);
    batchReader.reset(batch);
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
    SchemaBuilder builder = new SchemaBuilder();
    List<String> columnNames = batch.getColumns();
    for (int i = 0; i < columnNames.size(); i++) {
      String name = columnNames.get(i);
      builder.scalar(name, inferType(batch, format, name, i));
    }
    return builder.build();
  }

  private static ColumnType inferType(ScanResultValue batch, ScanQuery.ResultFormat format, String name, int index)
  {
    if (format == null) {
      return null;
    }
    switch (format) {
      case RESULT_FORMAT_LIST:
        return inferMapColumnType(batch, name);
      case RESULT_FORMAT_COMPACTED_LIST:
        return inferArrayColumnType(batch, index);
      default:
        throw new UOE("Result format not supported");
    }
  }

  private static ColumnType inferMapColumnType(ScanResultValue batch, String name)
  {
    List<Map<String, Object>> rows = batch.getRows();
    for (Map<String, Object> row : rows) {
      ColumnType type = inferType(row.get(name));
      if (type != null) {
        return type;
      }
    }
    return null;
  }

  private static ColumnType inferArrayColumnType(ScanResultValue batch, int index)
  {
    List<Object[]> rows = batch.getRows();
    for (Object[] row : rows) {
      ColumnType type = inferType(row[index]);
      if (type != null) {
        return type;
      }
    }
    return null;
  }

  private static ColumnType inferType(Object value)
  {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return ColumnType.STRING;
    }
    if (value instanceof Long) {
      return ColumnType.LONG;
    }
    if (value instanceof Double) {
      return ColumnType.DOUBLE;
    }
    if (value instanceof Float) {
      return ColumnType.FLOAT;
    }
    // TODO: Infer array types
    return ColumnType.UNKNOWN_COMPLEX;
  }

  private RowReader createReader(ScanQuery.ResultFormat format)
  {
    if (format == null) {
      // We don't know what this is, but we also have no rows. Special case.
      return new EmptyRowReader(this);
    }
    switch (format) {
      case RESULT_FORMAT_LIST:
        return new MapReader(this, i -> {
          List<Map<String, Object>> rows = batch.getRows();
          return rows.get(i);
        });
      case RESULT_FORMAT_COMPACTED_LIST:
        return new ObjectArrayReader(this, i -> {
          List<Object[]> rows = batch.getRows();
          return rows.get(i);
        });
      default:
        throw new UOE("Result format not supported");
    }
  }

  @Override
  public void reset(ScanResultValue value)
  {
    this.batch = value;
    reader.reset();
  }

  @Override
  public RowReader reader()
  {
    return reader;
  }

  @Override
  public int size()
  {
    return batch.getRows().size();
  }
}
