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

package org.apache.druid.exec.fragment;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.Iterators;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorProfile;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.NullOperator;
import org.apache.druid.exec.plan.OperatorSpec;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryTimeoutException;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class FragmentManager implements FragmentContext, Closeable
{
  public static class OperatorTracker
  {
    public final OperatorSpec plan;
    public final Operator operator;
    public OperatorProfile profile;

    private OperatorTracker(OperatorSpec plan, Operator op)
    {
      this.plan = plan;
      this.operator = op;
    }
  }

  private final QueryManager query;
  private final Map<Operator, OperatorTracker> operators = new IdentityHashMap<>();
  private final String queryId;
  private long startTimeMillis;
  private long closeTimeMillis;
  private long timeoutMs = Long.MAX_VALUE;
  private long timeoutAt;
  private final List<Exception> exceptions = new ArrayList<>();
  private final List<Consumer<FragmentManager>> closeListeners = new ArrayList<>();
  private State state = State.START;
  private Operator rootOperator;

  public FragmentManager(
      final QueryManager query,
      final String queryId
  )
  {
    this.query = query;
    this.queryId = queryId;
    this.startTimeMillis = System.currentTimeMillis();
  }

  public QueryManager query()
  {
    return query;
  }

  @Override
  public State state()
  {
    return state;
  }

  @Override
  public String queryId()
  {
    return queryId;
  }

  @SuppressWarnings("unused") // Needed in future steps
  public void setTimeout(long timeoutMs)
  {
    this.timeoutMs = timeoutMs;
    if (timeoutMs > 0) {
      this.timeoutAt = startTimeMillis + timeoutMs;
    } else {
      this.timeoutAt = JodaUtils.MAX_INSTANT;
    }
  }

  public void registerRoot(Operator op)
  {
    rootOperator = op;
  }

  /**
   * Register an operator for this fragment. The operator will be
   * closed automatically upon fragment completion both for the success
   * and error cases. An operator <i>may</i> be closed earlier, if a
   * DAG branch detects it is done during a run. Thus, every operator
   * must handle a call to {@code close()} when the operator is already
   * closed.
   *
   * Operators may be registered during a run, which is useful in the
   * conversion from query runners as sometimes the query runner decides
   * late what child to create.
   */
  public synchronized void register(OperatorSpec spec, Operator op)
  {
    Preconditions.checkState(state == State.START || state == State.RUN);
    OperatorTracker tracker = new OperatorTracker(spec, op);
    operators.put(op, tracker);
  }

  /**
   * Reports the exception, if any, that terminated the fragment.
   * Should be non-null only if the state is {@code FAILED}.
   */
  public Exception exception()
  {
    if (exceptions.isEmpty()) {
      return null;
    } else {
      return exceptions.get(0);
    }
  }

  @Override
  public boolean failed()
  {
    return !exceptions.isEmpty();
  }

  /**
   * Return the root, if it exists, as an operator. If the root is already an
   * operator, return it. If it is a sequence either unwrap it to get an
   * operator, or wrap it to get an operator. Returns {@code null} if there
   * is no root at all.
   */
  public <T> Operator rootOperator()
  {
    return rootOperator;
  }

  /**
   * Run the root operator (converted from a sequence if necessary) and return
   * the operator's {@link ResultIterator}. If there is no root (pathological case),
   * create one as a null operator.
   */
  public ResultIterator run()
  {
    Preconditions.checkState(state == State.START);
    if (rootOperator == null) {
      registerRoot(new NullOperator(this));
    }
    state = State.RUN;
    return rootOperator.open();
  }

  /**
   * Materializes the entire result set as a list. Primarily for testing.
   * Opens the fragment, reads results, and closes the fragment.
   */
  public List<Batch> toList()
  {
    try {
      if (rootOperator != null) {
        return Iterators.toList(run());
      } else {
        return Collections.emptyList();
      }
    }
    catch (RuntimeException e) {
      failed(e);
      throw e;
    }
    finally {
      close();
    }
  }

  public void failed(Exception exception)
  {
    this.exceptions.add(exception);
    this.state = State.FAILED;
  }

  @Override
  public void checkTimeout()
  {
    if (timeoutAt > 0 && System.currentTimeMillis() >= timeoutAt) {
      throw new QueryTimeoutException(
          StringUtils.nonStrictFormat("Query [%s] timed out after [%d] ms",
              queryId, timeoutMs));
    }
  }

  /**
   * Closes all operators from the leaves to the root.
   * As a result, operators must not call their children during
   * the {@code close()} call. Errors are collected, but all operators are closed
   * regardless of exceptions.
   */
  @Override
  public void close()
  {
    if (state == State.START) {
      state = State.CLOSED;
    }
    if (state == State.CLOSED) {
      return;
    }
    for (Operator op : operators.keySet()) {
      try {
        op.close(false);
      }
      catch (Exception e) {
        exceptions.add(e);
      }
    }
    state = State.CLOSED;
    closeTimeMillis = System.currentTimeMillis();
    for (Consumer<FragmentManager> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public synchronized void updateProfile(Operator op, OperatorProfile profile)
  {
    OperatorTracker tracker = operators.get(op);
    Preconditions.checkNotNull(tracker);
    tracker.profile = profile;
  }

  public void onClose(Consumer<FragmentManager> listener)
  {
    closeListeners.add(listener);
  }

  public long elapsedTimeMs()
  {
    return closeTimeMillis - startTimeMillis;
  }

  protected Map<Operator, OperatorTracker> operators()
  {
    return operators;
  }
}
