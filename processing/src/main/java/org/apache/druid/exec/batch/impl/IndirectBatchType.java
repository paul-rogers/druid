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

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.BatchCursor.BindableRowPositioner;

/**
 * Wraps a "base" batch with an index vector. The index vector can include
 * all rows, with a reordering (such as the result of a sort), or can include
 * a subset of rows (such as the result of a filter). The set of columns remains
 * unchanged: the indirect batch uses the column readers from the base batch.
 */
public class IndirectBatchType extends AbstractBatchType
{
  public static class IndirectData
  {
    public final Object data;
    public final int[] index;

    public IndirectData(Object data, int[] index)
    {
       this.data = data;
      this.index = index;
    }
  }

  private final BatchType baseType;

  public IndirectBatchType(BatchType baseType)
  {
    super(
        baseType.format(),
        true,  // Can seek
        false, // Can't sort
        true   // Can write (using the base type)
    );
    this.baseType = baseType;
  }

  public static IndirectBatchType of(BatchType base)
  {
    return new IndirectBatchType(base);
  }

  public static BatchSchema schemaOf(BatchSchema baseSchema)
  {
    return new BatchSchema(of(baseSchema.type()), baseSchema.rowSchema());
  }

  public static IndirectData wrap(Object data, int[] index)
  {
    return new IndirectData(data, index);
  }

  public BatchType baseType()
  {
    return baseType;
  }

  @Override
  public BatchCursor newCursor(RowSchema schema, BindableRowPositioner positioner)
  {
    return new IndirectBatchReader(batchSchema(schema), positioner);
  }

  @Override
  public BatchWriter<?> newWriter(RowSchema schema, int sizeLimit)
  {
    return baseType.newWriter(schema, sizeLimit);
  }

  @Override
  public void bindCursor(BatchCursor cursor, Object data)
  {
    ((IndirectBatchReader) cursor).bind((IndirectData) data);
  }

  @Override
  public int sizeOf(Object data)
  {
    return data == null ? 0 : ((IndirectData) data).index.length;
  }

  @Override
  public boolean canDirectCopyFrom(BatchType otherType)
  {
    return false;
  }
}
