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

import org.apache.druid.queryng.rows.RowReader.BindableRowReader;

import java.util.function.Supplier;

public abstract class AbstractRowReader<T> implements BindableRowReader
{
  private final RowSchema schema;
  private final ScalarColumnReader[] columnReaders;
  private final Supplier<T> rowSupplier;
  protected T row;

  public AbstractRowReader(final RowSchema schema, final Supplier<T> rowSupplier)
  {
    this.schema = schema;
    columnReaders = new ScalarColumnReader[schema.size()];
    this.rowSupplier = rowSupplier;
  }

  @Override
  public ScalarColumnReader scalar(String name)
  {
    int index = schema.ordinal(name);
    return index == -1 ? null : scalar(index);
  }

  @Override
  public ScalarColumnReader scalar(int ordinal)
  {
    if (columnReaders[ordinal] == null) {
      columnReaders[ordinal] = makeColumn(ordinal);
    }
    return columnReaders[ordinal];
  }

  protected abstract ScalarColumnReader makeColumn(int ordinal);

  @Override
  public void bind()
  {
    row = rowSupplier.get();
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }
}
