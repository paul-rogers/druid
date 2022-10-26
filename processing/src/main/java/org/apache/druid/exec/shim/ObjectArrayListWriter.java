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
import org.apache.druid.exec.batch.ColumnWriterFactory.ScalarColumnWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.impl.AbstractScalarWriter;
import org.apache.druid.exec.batch.impl.ColumnWriterFactoryImpl;

/**
 * Batch writer for a list of {@code Object} arrays where columns are represented
 * as values at an array index given by the associated schema.
 */
public class ObjectArrayListWriter extends ListWriter<Object[]>
{
  /**
   * Since column values are all objects, use a generic column writer.
   */
  private class ScalarWriterImpl extends AbstractScalarWriter
  {
    private final int index;

    public ScalarWriterImpl(int index)
    {
      this.index = index;
    }

    @Override
    public ColumnSchema schema()
    {
      return columns().schema().column(index);
    }

    @Override
    public void setObject(Object value)
    {
      row[index] = value;
    }
  }

  private final int rowWidth;
  private Object[] row;

  public ObjectArrayListWriter(RowSchema schema, int sizeLimit)
  {
    super(ObjectArrayListBatchType.INSTANCE.factory(schema), sizeLimit);
    this.rowWidth = schema.size();
    final ScalarColumnWriter[] columnWriters = new ScalarColumnWriter[rowWidth];
    for (int i = 0; i < rowWidth; i++) {
      columnWriters[i] = new ScalarWriterImpl(i);
    }
    this.columnWriters = new ColumnWriterFactoryImpl(schema, columnWriters);
  }

  @Override
  protected Object[] newInstance()
  {
    row = new Object[rowWidth];
    return row;
  }

  @Override
  public int directCopy(BatchReader from, int n)
  {
    ObjectArrayListReader source = from.unwrap(ObjectArrayListReader.class);
    if (source == null) {
      return super.directCopy(from, n);
    } else {
      return appendFromList(source, n);
    }
  }
}
