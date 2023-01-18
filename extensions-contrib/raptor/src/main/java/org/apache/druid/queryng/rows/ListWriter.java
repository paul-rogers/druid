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

import java.util.ArrayList;
import java.util.List;

public abstract class ListWriter<T> extends AbstractBatchWriter<List<T>>
{
  protected final int sizeLimit;

  public ListWriter(RowSchema schema)
  {
    this(schema, Integer.MAX_VALUE);
  }

  public ListWriter(RowSchema schema, int sizeLimit)
  {
    super(schema);
    this.sizeLimit = sizeLimit;
  }

  @Override
  public void newBatch()
  {
    batch = new ArrayList<>();
  }

  @Override
  public List<T> harvest()
  {
    List<T> result = batch;
    batch = null;
    return result;
  }

  @Override
  public int size()
  {
    return batch == null ? 0 : batch.size();
  }

  @Override
  public boolean isFull()
  {
    return size() >= sizeLimit;
  }

  @Override
  public boolean newRow()
  {
    if (isFull()) {
      return false;
    }
    batch.add(createRow());
    rowWriter.bind();
    return true;
  }

  protected abstract T createRow();
}
