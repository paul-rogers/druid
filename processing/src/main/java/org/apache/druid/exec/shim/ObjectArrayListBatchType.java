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
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.impl.AbstractBatchType;

import java.util.List;

/**
 * Batch that represents a list of {@code Object} arrays where columns are represented
 * as values at an array index given by the associated schema.
 */
public class ObjectArrayListBatchType extends AbstractBatchType
{
  public static final ObjectArrayListBatchType INSTANCE = new ObjectArrayListBatchType();

  public ObjectArrayListBatchType()
  {
    super(
        BatchType.BatchFormat.OBJECT_ARRAY,
        true,  // Can seek
        false, // Can't sort
        true   // Can write
    );
  }

  @Override
  public BatchReader newReader(RowSchema schema)
  {
    return new ObjectArrayListReader(schema);
  }

  @Override
  public BatchWriter<?> newWriter(RowSchema schema, int sizeLimit)
  {
    return new ObjectArrayListWriter(schema, sizeLimit);
  }

  @Override
  public void bindReader(BatchReader reader, Object data)
  {
    ((ObjectArrayListReader) reader).bind(cast(data));
  }

  @SuppressWarnings("unchecked")
  private List<Object[]> cast(Object data)
  {
    return (List<Object[]>) data;
  }

  @Override
  public int sizeOf(Object data)
  {
    return data == null ? 0 : cast(data).size();
  }

}
