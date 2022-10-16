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

import org.apache.druid.queryng.rows.Batch.ReadableBatch;
import org.apache.druid.queryng.rows.RowReader.RowIndex;

public abstract class AbstractRowReader implements RowReader, RowIndex
{
  interface RowSupplier<T>
  {
    T get(int row);
  }

  private final ReadableBatch batch;
  protected final ScalarColumnReader[] columnReaders;
  protected int posn = -1;

  public AbstractRowReader(final ReadableBatch batch)
  {
    this.batch = batch;
    columnReaders = new ScalarColumnReader[batch.schema().size()];
  }

  @Override
  public void reset()
  {
    this.posn = -1;
  }

  @Override
  public ScalarColumnReader scalar(String name)
  {
    int index = batch.schema().ordinal(name);
    return scalar(index);
  }

  @Override
  public boolean next()
  {
    if (++posn == batch.size()) {
      posn--;
      return false;
    }
    readRow();
    return true;
  }

  protected abstract void readRow();

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
  public int index()
  {
    return posn;
  }

  @Override
  public RowSchema schema()
  {
    return batch.schema();
  }
}
