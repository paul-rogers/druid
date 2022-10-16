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

import org.apache.druid.queryng.rows.Batch.WritableBatch;
import org.apache.druid.queryng.rows.RowSchema.ColumnSchema;

public class ObjectArrayWriter extends AbstractRowWriter
{
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
      return ObjectArrayWriter.this.schema().column(index);
    }

    @Override
    public void setObject(Object value)
    {
      row[index] = value;
    }
  }

  private final RowProvider<Object[]> rowProvider;
  private Object[] row;

  public ObjectArrayWriter(WritableBatch batch, RowProvider<Object[]> rowProvider)
  {
    super(batch);
    this.rowProvider = rowProvider;
    for (int i = 0; i < columnWriters.length; i++) {
      columnWriters[i] = new ScalarWriterImpl(i);
    }
  }

  @Override
  public boolean next()
  {
    row = rowProvider.newRow();
    return true;
  }
}
