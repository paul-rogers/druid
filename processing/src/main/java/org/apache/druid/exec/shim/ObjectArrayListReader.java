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

import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.operator.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.operator.RowSchema.ColumnSchema;
import org.apache.druid.exec.operator.impl.AbstractScalarReader;
import org.apache.druid.exec.operator.impl.ColumnReaderFactoryImpl;
import org.apache.druid.exec.operator.impl.ColumnReaderFactoryImpl.ColumnReaderMaker;

import java.util.List;

public class ObjectArrayListReader extends ListReader<Object[]> implements ColumnReaderMaker
{
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
      return columns().schema().column(index);
    }
  }

  protected Object[] row;

  public ObjectArrayListReader(RowSchema schema)
  {
    this.columnReaders = new ColumnReaderFactoryImpl(schema, this);
  }

  public static BatchReader of(RowSchema schema, List<Object[]> batch)
  {
    ObjectArrayListReader reader = new ObjectArrayListReader(schema);
    reader.bind(batch);
    return reader;
  }

  @Override
  protected void bindRow(int posn)
  {
    row = batch.get(posn);
  }

  @Override
  public ScalarColumnReader buildReader(int index)
  {
    return new ColumnReaderImpl(index);
  }
}
