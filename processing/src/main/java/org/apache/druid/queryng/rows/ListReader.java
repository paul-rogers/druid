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

import org.apache.druid.queryng.rows.Batch.AbstractBatch;
import org.apache.druid.queryng.rows.Batch.BatchReader;

import java.util.List;

public abstract class ListReader<T> extends AbstractBatch implements BatchReader<List<T>>
{
  protected List<T> batch;

  public ListReader(RowSchema schema)
  {
    super(schema);
  }

  @Override
  public void reset(List<T> batch)
  {
    this.batch = batch;
    reader().reset();
  }

  @Override
  public int size()
  {
    return batch == null ? 0 : batch.size();
  }
}
