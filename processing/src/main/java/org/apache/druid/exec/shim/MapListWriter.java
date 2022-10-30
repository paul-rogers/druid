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
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Batch writer for a list of {@link Map}s where columns are represented
 * as key/value pairs.
 */
public class MapListWriter extends ListWriter<Map<String, Object>>
{
  private class RowWriterImpl implements RowWriter
  {
    private final ScalarColumnReader projections[];

    private RowWriterImpl(ScalarColumnReader[] projections)
    {
      this.projections = projections;
    }

    @Override
    public boolean write()
    {
      if (isFull()) {
        return false;
      }
      Map<String, Object> row = newRow();
      RowSchema schema = batchSchema.rowSchema();
      for (int i = 0; i < projections.length; i++) {
        if (!projections[i].isNull()) {
          row.put(schema.column(i).name(), projections[i].getValue());
        }
      }
      return true;
    }
  }

  public MapListWriter(RowSchema schema, int sizeLimit)
  {
    super(MapListBatchType.INSTANCE.batchSchema(schema), sizeLimit);
  }

  protected Map<String, Object> newRow()
  {
    Map<String, Object> row = new HashMap<>();
    batch.add(row);
    return row;
  }

  @Override
  protected RowWriter newRowWriter(ScalarColumnReader[] projections)
  {
    return new RowWriterImpl(projections);
  }

  @Override
  public Copier copier(BatchReader from)
  {
    MapListReader source = from.unwrap(MapListReader.class);
    if (source == null) {
      return super.copier(from);
    } else {
      return new CopierImpl(source);
    }
  }
}
