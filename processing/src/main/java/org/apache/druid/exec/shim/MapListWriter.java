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
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.operator.ColumnWriterFactory.ScalarColumnWriter;
import org.apache.druid.exec.operator.RowSchema.ColumnSchema;
import org.apache.druid.exec.operator.impl.AbstractScalarWriter;
import org.apache.druid.exec.operator.impl.ColumnWriterFactoryImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapListWriter extends ListWriter<Map<String, Object>>
{
  private class ScalarWriterImpl extends AbstractScalarWriter
  {
    private final String colName;

    public ScalarWriterImpl(String colName)
    {
      this.colName = colName;
    }

    @Override
    public ColumnSchema schema()
    {
      return columns().schema().column(colName);
    }

    @Override
    public void setObject(Object value)
    {
      row.put(colName, value);
    }
  }

  private Map<String, Object> row;

  public MapListWriter(RowSchema schema)
  {
    this(schema, Integer.MAX_VALUE);
  }

  public MapListWriter(RowSchema schema, int sizeLimit)
  {
    super(sizeLimit);
    int rowWidth = schema.size();
    final ScalarColumnWriter[] columnWriters = new ScalarColumnWriter[rowWidth];
    for (int i = 0; i < rowWidth; i++) {
      columnWriters[i] = new ScalarWriterImpl(schema.column(i).name());
    }
    this.columnWriters = new ColumnWriterFactoryImpl(schema, columnWriters);
  }

  @Override
  protected Map<String, Object> newInstance()
  {
    row = new HashMap<>();
    return row;
  }

  @Override
  protected Batch wrapBatch(List<Map<String, Object>> batch)
  {
    return new MapListBatch(columns().schema(), batch);
  }

  @Override
  public boolean canDirectCopyFrom(BatchReader reader)
  {
    return reader.unwrap(MapListReader.class) != null;
  }

  @Override
  public int directCopy(BatchReader from, int n)
  {
    MapListReader source = from.unwrap(MapListReader.class);
    if (source == null) {
      return super.directCopy(from, n);
    } else {
      return appendFromList(source, n);
    }
  }
}
