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

import org.apache.druid.exec.batch.BatchFactory;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.ColumnReaderFactory;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.util.ExecUtils;

/**
 * Defines a reader which wraps some other reader. The column readers for
 * and cursor for this reader are the same the the same as those for the
 * delegate. Use this when some row format is a "wrapped" version of some
 * simpler row format, such as the {@code ScanResultValue} wraps a list of
 * maps or object arrays.
 */
public abstract class DelegatingBatchReader implements BatchReader
{
  protected final BatchFactory factory;

  public DelegatingBatchReader(BatchFactory factory)
  {
    this.factory = factory;
  }

  protected abstract BatchReader delegate();

  @Override
  public BatchFactory factory()
  {
    return factory;
  }

  @Override
  public ColumnReaderFactory columns()
  {
    return delegate().columns();
  }

  @Override
  public RowCursor cursor()
  {
    return delegate().cursor();
  }

  @Override
  public BatchCursor batchCursor()
  {
    return delegate().batchCursor();
  }

  @Override
  public <T> T unwrap(Class<T> readerClass)
  {
    T reader = ExecUtils.unwrap(this, readerClass);
    return reader != null ? reader : delegate().unwrap(readerClass);
  }

  @Override
  public RowSchema schema()
  {
    return delegate().schema();
  }
}
