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

package org.apache.druid.exec.test;

import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.AbstractUnaryBatchOperator;

/**
 * For testing: insert empty batches every other batch.
 */
public class EmptyBatchOperator extends AbstractUnaryBatchOperator
    implements ResultIterator<Object>
{
  private final BatchWriter<?> writer;
  private int batchCount;

  public EmptyBatchOperator(FragmentContext context, BatchOperator input)
  {
    super(context, input.batchSchema(), input);
    this.writer = input.batchSchema().newWriter(10);
  }

  @Override
  public ResultIterator<Object> open()
  {
    openInput();
    return this;
  }

  @Override
  public Object next() throws EofException
  {
    if (batchCount % 2 == 1) {
      batchCount++;
      writer.newBatch();
      return writer.harvest();
    }
    Object data = inputIter.next();
    batchCount++;
    return data;
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      closeInput();
    }
  }
}
