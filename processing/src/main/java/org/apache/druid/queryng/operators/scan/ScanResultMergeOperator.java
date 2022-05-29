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

package org.apache.druid.queryng.operators.scan;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Performs an n-way merge on n ordered child operators.
 * <p>
 * Returns elements from the priority queue in order of increasing priority, here
 * defined as in the desired output order.
 * This is due to the fact that PriorityQueue#remove() polls from the head of the queue which is, according to
 * the PriorityQueue javadoc, "the least element with respect to the specified ordering"
 *
 * @see {@link org.apache.druid.java.util.common.guava.MergeSequence}
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#nWayMergeAndLimit}
*/
public class ScanResultMergeOperator implements IterableOperator
{
  public static ScanResultMergeOperator forQuery(ScanQuery query, List<Operator> children)
  {
    return new ScanResultMergeOperator(query.getResultOrdering(), children);
  }

  private static class Entry
  {
    final Operator child;
    final Iterator<Object> childIter;
    ScanResultValue row;

    public Entry(Operator child, Iterator<Object> childIter, ScanResultValue row)
    {
      this.child = child;
      this.childIter = childIter;
      this.row = row;
    }
  }

  private final List<Operator> children;
  private final PriorityQueue<Entry> pQueue;

  public ScanResultMergeOperator(Ordering<ScanResultValue> ordering, List<Operator> children)
  {
    this.children = children;
    this.pQueue = new PriorityQueue<>(
        32,
        ordering.onResultOf(
            (Function<Entry, ScanResultValue>) input -> input.row
        )
    );
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    for (Operator child : children) {
      Iterator<Object> childIter = child.open(context);
      if (childIter.hasNext()) {
        pQueue.add(new Entry(child, childIter, (ScanResultValue) childIter.next()));
      } else {
        child.close(true);
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
  public Object next()
  {
    Entry entry = pQueue.remove();
    Object row = entry.row;
    if (entry.childIter.hasNext()) {
      entry.row = (ScanResultValue) entry.childIter.next();
      pQueue.add(entry);
    } else {
      entry.child.close(true);
    }
    return row;
  }

  @Override
  public void close(boolean cascade)
  {
    if (!cascade) {
      return;
    }
    while (!pQueue.isEmpty()) {
      pQueue.remove().child.close(cascade);
    }
  }
}
