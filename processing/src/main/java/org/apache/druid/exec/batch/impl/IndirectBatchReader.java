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

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.ColumnReaderFactory;
import org.apache.druid.exec.batch.impl.IndirectBatchType.IndirectData;

public class IndirectBatchReader extends AbstractBatchReader
{
  private final BatchType baseType;
  private final BatchReader baseReader;
  private int[] index;

  public IndirectBatchReader(BatchSchema factory)
  {
    super(factory);
    IndirectBatchType batchType = (IndirectBatchType) factory.type();
    this.baseType = batchType.baseType();
    this.baseReader = baseType.newReader(factory.rowSchema());
  }

  public void bind(IndirectData data)
  {
    baseType.bindReader(baseReader, data.data);
    this.index = data.index;
    cursor.bind(index.length);
  }

  @Override
  protected void bindRow(int posn)
  {
    baseReader.batchCursor().seek(index[posn]);
  }

  @Override
  public ColumnReaderFactory columns()
  {
    return baseReader.columns();
  }
}
