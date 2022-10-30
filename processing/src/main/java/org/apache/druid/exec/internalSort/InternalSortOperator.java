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

package org.apache.druid.exec.internalSort;

import com.google.common.base.Stopwatch;
import org.apache.druid.exec.batch.impl.IndirectBatchType;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.operator.OperatorProfile;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.AbstractUnaryBatchOperator;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.frame.key.SortColumn;

import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class InternalSortOperator
    extends AbstractUnaryBatchOperator
    implements ResultIterator<Object>
{
  protected final List<SortColumn> keys;
  private ResultIterator<Object> resultIter;
  protected int rowCount;
  protected int batchCount;
  protected long sortTimeMs;

  public InternalSortOperator(FragmentContext context, InternalSortOp plan, BatchOperator input)
  {
    super(
        context,

        // The return batch type is the same as the input, with
        // an indirection vector added.
        IndirectBatchType.schemaOf(input.batchSchema()),
        input
    );
    this.keys = plan.keys();
  }

  @Override
  public ResultIterator<Object> open()
  {
    openInput();
    resultIter = () -> sort();
    return this;
  }

  private Object sort() throws EofException
  {
    Stopwatch stopwatch = Stopwatch.createStarted();
    resultIter = doSort();
    sortTimeMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    return resultIter.next();
  }

  protected abstract ResultIterator<Object> doSort() throws EofException;

  @Override
  public Object next() throws EofException
  {
    return resultIter.next();
  }

  @Override
  public void close(boolean cascade)
  {
    closeInput();
    resultIter = null;
    OperatorProfile profile = new OperatorProfile("Internal Sort");
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    profile.add(OperatorProfile.BATCH_COUNT_METRIC, batchCount);
    profile.add("sortTimeMs", sortTimeMs);
    context.updateProfile(this, profile);
  }
}
