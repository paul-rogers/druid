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
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.impl.IndirectBatchType.IndirectData;

public class IndirectBatchReader extends AbstractBatchCursor
{
  private final BatchType baseType;
  private final BatchCursor baseCursor;
  private int[] index;

  public IndirectBatchReader(BatchSchema factory, BindableRowPositioner positioner)
  {
    super(factory, positioner);
    IndirectBatchType batchType = (IndirectBatchType) factory.type();
    this.baseType = batchType.baseType();
    this.baseCursor = baseType.newCursor(factory.rowSchema());
  }

  public void bind(IndirectData data)
  {
    baseType.bindCursor(baseCursor, data.data);
    this.index = data.index;
    positioner.bind(index.length);
  }

  @Override
  public void updatePosition(int posn)
  {
    baseCursor.positioner().seek(posn == -1 ? posn : index[posn]);
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return baseCursor.columns();
  }
}
