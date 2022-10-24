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

import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.ColumnWriterFactory;

/**
 * Defines a writer which wraps some other writer. The column writers for
 * this writer are the same the the same as those for the
 * delegate. Use this when some row format is a "wrapped" version of some
 * simpler row format, such as the {@code ScanResultValue} wraps a list of
 * maps or object arrays.
 */
public abstract class DelegatingBatchWriter implements BatchWriter
{
  protected final BatchWriter delegate;

  public DelegatingBatchWriter(BatchWriter delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public void newBatch()
  {
    delegate.newBatch();
  }

  @Override
  public int size()
  {
    return delegate.size();
  }

  @Override
  public boolean isFull()
  {
    return delegate.isFull();
  }

  @Override
  public boolean newRow()
  {
    return delegate.newRow();
  }

  @Override
  public ColumnWriterFactory columns()
  {
    return delegate.columns();
  }
}
