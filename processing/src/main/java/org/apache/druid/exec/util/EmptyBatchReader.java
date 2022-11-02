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

package org.apache.druid.exec.util;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.impl.AbstractScalarReader;
import org.apache.druid.exec.batch.impl.BaseBatchCursor;
import org.apache.druid.exec.batch.impl.ColumnReaderFactoryImpl;
import org.apache.druid.exec.batch.impl.ColumnReaderFactoryImpl.ColumnReaderMaker;
import org.apache.druid.exec.batch.impl.SimpleRowPositioner;

/**
 * Trivial reader for an empty batch. Most useful when the format of the batch
 * is unknown because every format behaves the same when empty.
 */
public class EmptyBatchReader<T> extends BaseBatchCursor<T> implements ColumnReaderMaker
{
  private class ColumnReaderImpl extends AbstractScalarReader
  {
    private final int index;

    public ColumnReaderImpl(int index)
    {
      this.index = index;
    }

    @Override
    public ColumnSchema schema()
    {
      return columns().schema().column(index);
    }

    @Override
    public Object getObject()
    {
      return null;
    }
  }

  public EmptyBatchReader(BatchSchema factory)
  {
    super(factory, new SimpleRowPositioner());
    this.columnReaders = new ColumnReaderFactoryImpl(factory.rowSchema(), this);
  }

  @Override
  public ScalarColumnReader buildReader(int index)
  {
    return new ColumnReaderImpl(index);
  }

  @Override
  public void updatePosition(int posn)
  {
  }

  @Override
  protected void reset()
  {
  }
}
