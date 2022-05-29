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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.queryng.Timer;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;

import java.util.Iterator;
import java.util.function.ObjLongConsumer;

/**
 * Operator to emit runtime metrics. This is a temporary solution: these
 * metrics are better emitted at the top of the stack by the fragment
 * runner to avoid the per-row overhead.
 *
 * @see {@link org.apache.druid.query.MetricsEmittingQueryRunner}
 */
public class MetricsOperator implements Operator
{
  private static final Logger log = new Logger(MetricsOperator.class);

  private final Timer waitTimer;
  private final ServiceEmitter emitter;
  private final QueryMetrics<?> queryMetrics;
  private final ObjLongConsumer<? super QueryMetrics<?>> reportMetric;
  private final Operator child;
  private final Timer runTimer = Timer.create();
  private FragmentContext context;
  private State state = State.START;

  public MetricsOperator(
      final ServiceEmitter emitter,
      final QueryMetrics<?> queryMetrics,
      final ObjLongConsumer<? super QueryMetrics<?>> reportMetric,
      final Timer waitTimer,
      final Operator child
  )
  {
    this.emitter = emitter;
    this.queryMetrics = queryMetrics;
    this.reportMetric = reportMetric;
    this.waitTimer = waitTimer;
    this.child = child;
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    Preconditions.checkState(state == State.START);
    this.context = context;
    state = State.RUN;
    runTimer.start();
    return child.open(context);
  }

  @Override
  public void close(boolean cascade)
  {
    if (state != State.RUN) {
      state = State.CLOSED;
      return;
    }
    state = State.CLOSED;
    if (cascade) {
      child.close(cascade);
    }
    if (context.state() == FragmentContext.State.FAILED) {
      queryMetrics.status("failed");
    }
    reportMetric.accept(queryMetrics, runTimer.get());

    // Wait time is reported only in the outer-most metric operator.
    if (waitTimer != null) {
      queryMetrics.reportWaitTime(waitTimer.get() - runTimer.get());
    }
    try {
      queryMetrics.emit(emitter);
    }
    catch (Exception e) {
      // Query should not fail, because of emitter failure. Swallowing the exception.
      log.error("Failure while trying to emit [%s] with stacktrace [%s]", emitter.toString(), e);
    }
  }
}
