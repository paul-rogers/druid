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

package org.apache.druid.queryng.operators;

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.PriorityQueue;

/**
 * Performs an n-way merge on n ordered child operators.
 * Ordering is given by an {@link Ordering} which defines
 * the set of fields to order by, and their comparison
 * functions.
 * <p>
 * The form of the entry is defined by a subclass.
 *
 * @see {@link org.apache.druid.query.RetryQueryRunner}
 * @see {@link org.apache.druid.java.util.common.guava.MergeSequence}
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#nWayMergeAndLimit}
 */
public abstract class AbstractMergeOperator<T> implements IterableOperator<T>
{
  /**
   * Supplier of input rows. Holds the input operator and the current
   * (look-ahead) row for each operator.
   */
  public static class Input<T>
  {
    public final Operator<T> child;
    public final ResultIterator<T> childIter;
    public T row;

    public Input(
        Operator<T> child,
        ResultIterator<T> childIter,
        T row
    )
    {
      this.child = child;
      this.childIter = childIter;
      this.row = row;
    }

    public boolean next()
    {
      try {
        row = childIter.next();
        return true;
      }
      catch (EofException e) {
        close();
        return false;
      }
    }

    public T get()
    {
      return row;
    }

    public void close()
    {
      row = null;
      child.close(true);
    }
  }

  protected final FragmentContext context;
  protected final PriorityQueue<Input<T>> pQueue;
  protected State state = State.START;
  private int rowCount;

  public AbstractMergeOperator(
      FragmentContext context,
      Ordering<? super T> ordering,
      int approxInputCount
  )
  {
    this.context = context;
    this.pQueue = new PriorityQueue<>(
        approxInputCount == 0 ? 1 : approxInputCount,
        ordering.onResultOf(input -> input.get())
    );
    context.register(this);
  }

  @Override
  public T next() throws EofException
  {
    if (pQueue.isEmpty()) {
      throw Operators.eof();
    }
    rowCount++;
    Input<T> entry = pQueue.remove();
    T row = entry.get();
    if (entry.next()) {
      pQueue.add(entry);
    }
    return row;
  }

  @Override
  public void close(boolean cascade)
  {
    while (!pQueue.isEmpty()) {
      Input<T> input = pQueue.remove();
      if (cascade) {
        input.close();
      }
    }
    if (state == State.RUN) {
      OperatorProfile profile = new OperatorProfile("ordered-merge");
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
      context.updateProfile(this, profile);
    }
  }
}
