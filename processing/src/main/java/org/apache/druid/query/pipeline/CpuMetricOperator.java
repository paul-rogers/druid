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
import java.util.function.Supplier;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.Operator.IterableOperator;
import org.apache.druid.utils.JvmUtils;

/**
 * Measures CPU time of each child operation. Excludes CPU consumed
 * by the downstream consumer of this operator.
 *
 * @see {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
 */
public class CpuMetricOperator implements IterableOperator
{
  private final AtomicLong cpuTimeAccumulator;
  private final Supplier<Operator> inputSupplier;
  private final QueryMetrics<?> queryMetrics;
  private final ServiceEmitter emitter;
  private FragmentContext context;
  private Iterator<Object> childIter;
  private State state = State.START;

  public CpuMetricOperator(
      final AtomicLong cpuTimeAccumulator,
      final QueryMetrics<?> queryMetrics,
      final ServiceEmitter emitter,
      final Supplier<Operator> inputSupplier)
  {
    this.cpuTimeAccumulator = cpuTimeAccumulator == null ? new AtomicLong(0L) : cpuTimeAccumulator;
    this.queryMetrics = queryMetrics;
    this.emitter = emitter;
    this.inputSupplier = inputSupplier;
  }

  @Override
  public Iterator<Object> open(FragmentContext context) {
    this.context = context;
    childIter = inputSupplier.get().open(context);
    state = State.RUN;
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
    if (state != State.RUN) {
      state = State.CLOSED;
      return;
    }
    if (childIter != null && cascade) {
      inputSupplier.get().close(cascade);
    }
    childIter = null;
    final long cpuTimeNs = cpuTimeAccumulator.get();
    if (cpuTimeNs > 0) {
      context.responseContext().add(ResponseContext.Key.CPU_CONSUMED_NANOS, cpuTimeNs);
      queryMetrics.reportCpuTime(cpuTimeNs).emit(emitter);
    }
  }
}
