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

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.impl.AbstractScalarReader;
import org.apache.druid.exec.batch.impl.ColumnReaderFactoryImpl;
import org.apache.druid.exec.batch.impl.ColumnReaderFactoryImpl.ColumnReaderMaker;

import java.util.List;
import java.util.Map;

/**
 * Batch reader for a list of {@link Map}s where columns are represented
 * as key/value pairs.
 */
public class MapListReader extends ListReader<Map<String, Object>> implements ColumnReaderMaker
{
  /**
   * Since column values are all objects, use a generic column reader.
   */
  class ColumnReaderImpl extends AbstractScalarReader
  {
    private final String name;

    public ColumnReaderImpl(String name)
    {
      this.name = name;
    }

    @Override
    public boolean isNull()
    {
      return row == null || super.isNull();
    }

    @Override
    public Object getObject()
    {
      return row.get(name);
    }

    @Override
    public ColumnSchema schema()
    {
      return columns().schema().column(name);
    }
  }

  public MapListReader(RowSchema schema)
  {
    super(MapListBatchType.INSTANCE.batchSchema(schema));
    this.columnReaders = new ColumnReaderFactoryImpl(schema, this);
  }

  public static BatchReader of(RowSchema schema, List<Map<String, Object>> batch)
  {
    MapListReader reader = new MapListReader(schema);
    reader.bind(batch);
    return reader;
  }

  @Override
  public ScalarColumnReader buildReader(int index)
  {
    return new ColumnReaderImpl(schema.rowSchema().column(index).name());
  }
}
