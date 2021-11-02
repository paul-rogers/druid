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

package org.apache.druid.query.pipeline;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.Operator.IterableOperator;
import org.apache.druid.utils.JvmUtils;

/**
 * Measures CPU time of each child operation. Excludes CPU consumed
 * the downstream reader of this operator.
 *
 * @see {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
 */
public class CpuMetricOperator implements IterableOperator
{
  private final AtomicLong cpuTimeAccumulator;
  private final Operator child;
  private Iterator<Object> childIter;

  public CpuMetricOperator(final AtomicLong cpuTimeAccumulator, final Operator child)
  {
    this.cpuTimeAccumulator = cpuTimeAccumulator;
    this.child = child;
  }

  @Override
  public Iterator<Object> open(FragmentContext context) {
    childIter = child.open(context);
    return this;
  }

  @Override
  public boolean hasNext() {
    final long startRun = JvmUtils.getCurrentThreadCpuTime();
    try
    {
      return childIter != null && childIter.hasNext();
    }
    finally {
      cpuTimeAccumulator.addAndGet(JvmUtils.getCurrentThreadCpuTime() - startRun);
    }
  }

  @Override
  public Object next() {
    final long startRun = JvmUtils.getCurrentThreadCpuTime();
    try
    {
      return childIter.next();
    }
    finally {
      cpuTimeAccumulator.addAndGet(JvmUtils.getCurrentThreadCpuTime() - startRun);
    }
  }

  @Override
  public void close(boolean cascade) {
    if (childIter != null && cascade) {
      child.close(cascade);
    }
    childIter = null;
  }
}
