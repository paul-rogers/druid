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

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.impl.BatchImpl;

import java.util.List;

/**
 * Defines a writer which wraps some other writer. The column writers for
 * this writer are the same the the same as those for the
 * delegate. Use this when some row format is a "wrapped" version of some
 * simpler row format, such as the {@code ScanResultValue} wraps a list of
 * maps or object arrays.
 */
public abstract class DelegatingBatchWriter<T> implements BatchWriter<T>
{
  protected final BatchSchema factory;
  protected final BatchWriter<?> delegate;

  public DelegatingBatchWriter(final BatchSchema schema, final BatchWriter<?> delegate)
  {
    this.factory = schema;
    this.delegate = delegate;
  }

  @Override
  public BatchSchema schema()
  {
    return factory;
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
  public RowWriter rowWriter(List<ScalarColumnReader> readers)
  {
    return delegate.rowWriter(readers);
  }

  @Override
  public Copier copier(BatchReader source)
  {
    return delegate.copier(source);
  }

  @Override
  public Batch harvestAsBatch()
  {
    return new BatchImpl(factory, harvest());
  }
}
