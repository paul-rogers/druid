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

package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.Batches;

public class BatchImpl implements Batch
{
  private final BatchSchema schema;
  private Object data;

  public BatchImpl(BatchSchema factory)
  {
    this.schema = factory;
  }

  public BatchImpl(BatchSchema factory, Object data)
  {
    this(factory);
    this.data = data;
  }

  @Override
  public BatchSchema schema()
  {
    return schema;
  }

  @Override
  public void bind(Object data)
  {
    this.data = data;
  }

  @Override
  public BatchCursor newCursor()
  {
    BatchCursor cursor = Batches.toCursor(schema.newReader());
    bindCursor(cursor);
    return cursor;
  }

  @Override
  public void bindCursor(BatchCursor cursor)
  {
    schema.type().bindReader(cursor.reader(), data);
  }

  @Override
  public int size()
  {
    return schema.type().sizeOf(data);
  }

  @Override
  public Object data()
  {
    return data;
  }
}
