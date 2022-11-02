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

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;

/**
 * Batch writer for a list of {@code Object} arrays where columns are represented
 * as values at an array index given by the associated schema.
 */
public class ObjectArrayListWriter extends ListWriter<Object[]>
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
      Object[] row = newRow();
      for (int i = 0; i < projections.length; i++) {
        if (projections[i].isNull()) {
          row[i] = null;
        } else {
          row[i] = projections[i].getValue();
        }
      }
      return true;
    }
  }

  private final int rowWidth;

  public ObjectArrayListWriter(RowSchema schema, int sizeLimit)
  {
    super(ObjectArrayListBatchType.INSTANCE.batchSchema(schema), sizeLimit);
    this.rowWidth = schema.size();
  }

  protected Object[] newRow()
  {
    Object[]row = new Object[rowWidth];
    batch.add(row);
    return row;
  }

  @Override
  protected RowWriter newRowWriter(ScalarColumnReader[] projections)
  {
    return new RowWriterImpl(projections);
  }

  @Override
  public Copier copier(BatchCursor from)
  {
    ObjectArrayListCursor source = from.unwrap(ObjectArrayListCursor.class);
    if (source == null) {
      return super.copier(from);
    } else {
      return new CopierImpl(source);
    }
  }
}
