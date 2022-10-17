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

public abstract class AbstractBatchReader<T> extends AbstractBatch implements BatchReader<T>
{
  protected T batch;
  protected int posn = -1;
  protected BindableRowReader rowReader;

  public AbstractBatchReader(final RowSchema schema)
  {
    super(schema);
  }

  @Override
  public void bind(T batch)
  {
    this.batch = batch;
    reset();
  }

  @Override
  public void reset()
  {
    this.posn = -1;
  }

  @Override
  public boolean next()
  {
    if (++posn >= size()) {
      posn = size();
      return false;
    }
    rowReader.bind();
    return true;
  }

  @Override
  public boolean seek(int newPosn)
  {
    // Bound the new position to the valid range. If the
    // batch is empty, the new position will be -1: before the
    // (non-existent) first row.
    if (newPosn < 0) {
      posn = -1;
      return false;
    } else if (newPosn >= size()) {
      posn = size();
      return false;
    }
    posn = newPosn;
    rowReader.bind();
    return true;
  }

  @Override
  public int index()
  {
    return posn;
  }

  @Override
  public RowReader row()
  {
    return rowReader;
  }
}
