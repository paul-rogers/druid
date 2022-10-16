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

import org.apache.druid.queryng.rows.Batch.WritableBatch;

public abstract class AbstractRowWriter implements RowWriter
{
  public interface RowProvider<T>
  {
    T newRow();
  }

  protected final WritableBatch batch;
  protected final ScalarColumnWriter[] columnWriters;

  public AbstractRowWriter(WritableBatch batch)
  {
    this.batch = batch;
    this.columnWriters = new ScalarColumnWriter[batch.schema().size()];
  }

  @Override
  public RowSchema schema()
  {
    return batch.schema();
  }

  @Override
  public ScalarColumnWriter scalar(String name)
  {
    return scalar(schema().ordinal(name));
  }

  @Override
  public ScalarColumnWriter scalar(int ordinal)
  {
    return columnWriters[ordinal];
  }

  @Override
  public void reset()
  {
  }
}
