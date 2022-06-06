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

package org.apache.druid.queryng.operators.general;

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Perform an in-memory, n-way merge on n ordered inputs.
 * Ordering is given by an {@link Ordering} which defines
 * the set of fields to order by, and their comparison
 * functions.
 */
public class OrderedMergeOperator<T> implements IterableOperator<T>
{
  /**
   * Manages a single input characterized by the operator for that
   * input, an iterator over that operator, and the current value
   * for that iterator. This class caches the current value so that
   * the priority queue can perform comparisons on it.
   */
  private static class Input<T>
  {
    private final Operator<T> input;
    private final Iterator<T> iter;
    T currentValue;

    private Input(Operator<T> input)
    {
      this.input = input;
      this.iter = input.open();
      if (iter.hasNext()) {
        currentValue = iter.next();
      } else {
        currentValue = null;
        input.close(true);
      }
    }

    private T get()
    {
      return currentValue;
    }

    private boolean next()
    {
      if (iter.hasNext()) {
        currentValue = iter.next();
        return true;
      } else {
        currentValue = null;
        input.close(true);
        return false;
      }
    }

    private boolean eof()
    {
      return currentValue == null;
    }

    private void close(boolean cascade)
    {
      if (currentValue != null) {
        currentValue = null;
        input.close(cascade);
      }
    }
  }

  private final List<Operator<T>> inputs;
  private final PriorityQueue<Input<T>> pQueue;

  public OrderedMergeOperator(
      FragmentContext context,
      Ordering<? super T> ordering,
      List<Operator<T>> inputs
  )
  {
    this.inputs = inputs;
    int inputCount = inputs.size();
    this.pQueue = new PriorityQueue<>(
        inputCount == 0 ? 1 : inputCount,
        ordering.onResultOf(input -> input.get())
    );
    context.register(this);
  }

  @Override
  public Iterator<T> open()
  {
    for (Operator<T> inputOper : inputs) {
      Input<T> input = new Input<>(inputOper);
      if (!input.eof()) {
        pQueue.add(input);
      }
    }
    return this;
  }

  @Override
  public boolean hasNext()
  {
    return !pQueue.isEmpty();
  }

  @Override
  public T next()
  {
    Input<T> input = pQueue.remove();
    T result = input.get();
    if (input.next()) {
      pQueue.add(input);
    }
    return result;
  }

  @Override
  public void close(boolean cascade)
  {
    while (!pQueue.isEmpty()) {
      Input<T> input = pQueue.remove();
      input.close(cascade);
    }
  }
}
