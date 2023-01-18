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
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BindingListener;
import org.apache.druid.exec.batch.ColumnReaderProvider;
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
  protected final BatchSchema factory;

  public DelegatingBatchReader(BatchSchema schema)
  {
    this.factory = schema;
  }

  protected abstract BatchReader delegate();

  @Override
  public BatchSchema schema()
  {
    return factory;
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return delegate().columns();
  }

  @Override
  public int size()
  {
    return delegate().size();
  }

  @Override
  public void bindListener(BindingListener listener)
  {
    delegate().bindListener(listener);
  }

  @Override
  public void updatePosition(int posn)
  {
    delegate().updatePosition(posn);
  }

  @Override
  public <T> T unwrap(Class<T> cursorClass)
  {
    T reader = ExecUtils.unwrap(this, cursorClass);
    return reader != null ? reader : delegate().unwrap(cursorClass);
  }
}
