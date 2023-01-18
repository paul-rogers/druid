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

public abstract class DelegatingBatchReader<T> implements BatchReader<T>
{
  protected abstract BatchReader<?> delegate();

  @Override
  public RowSchema schema()
  {
    return delegate().schema();
  }

  @Override
  public int size()
  {
    return delegate().size();
  }

  @Override
  public void reset()
  {
    delegate().reset();
  }

  @Override
  public int index()
  {
    return delegate().index();
  }

  @Override
  public boolean seek(int posn)
  {
    return delegate().seek(posn);
  }

  @Override
  public boolean next()
  {
    return delegate().next();
  }

  @Override
  public RowReader row()
  {
    return delegate().row();
  }
}
