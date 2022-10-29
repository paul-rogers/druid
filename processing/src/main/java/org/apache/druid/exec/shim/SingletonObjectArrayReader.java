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

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.impl.AbstractScalarReader;
import org.apache.druid.exec.batch.impl.BaseDirectReader;
import org.apache.druid.exec.batch.impl.ColumnReaderFactoryImpl;
import org.apache.druid.exec.batch.impl.ColumnReaderFactoryImpl.ColumnReaderMaker;

/**
 * Specialized batch reader for a single object array. Used when
 * the data for a row is created programmatically rather than via
 * a transform from another batch. Primarily for testing.
 */
public class SingletonObjectArrayReader extends BaseDirectReader implements ColumnReaderMaker
{
  /**
   * Since column values are all objects, use a generic column reader.
   */
  class ColumnReaderImpl extends AbstractScalarReader
  {
    private final int index;

    public ColumnReaderImpl(int index)
    {
      this.index = index;
    }

    @Override
    public Object getObject()
    {
      return row[index];
    }

    @Override
    public ColumnSchema schema()
    {
      return SingletonObjectArrayReader.this.schema().column(index);
    }
  }

  protected Object[] row;

  public SingletonObjectArrayReader(RowSchema schema)
  {
    super(SingletonObjectArrayBatchType.INSTANCE.factory(schema));
    this.columnReaders = new ColumnReaderFactoryImpl(schema, this);
    this.cursor.bind(0);
  }

  public void bind(Object[] row)
  {
    this.row = row;
    cursor.bind(1);
  }

  @Override
  protected void bindRow(int posn)
  {
    Preconditions.checkArgument(posn == 0);
  }

  @Override
  public ScalarColumnReader buildReader(int index)
  {
    return new ColumnReaderImpl(index);
  }
}
