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

import org.apache.druid.queryng.rows.RowSchema.ColumnSchema;

import java.util.function.Supplier;

public class ObjectArrayReader extends AbstractRowReader<Object[]>
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
      return ObjectArrayReader.this.schema().column(index);
    }
  }

  public ObjectArrayReader(final RowSchema schema, Supplier<Object[]> rowSupplier)
  {
    super(schema, rowSupplier);
  }

  @Override
  protected ScalarColumnReader makeColumn(int ordinal)
  {
    return new ColumnReaderImpl(ordinal);
  }
}
