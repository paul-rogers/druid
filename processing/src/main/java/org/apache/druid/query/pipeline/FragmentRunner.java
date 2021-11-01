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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.ResponseContext;

import com.google.common.base.Preconditions;

public class FragmentRunner
{
  public static final long NO_TIMEOUT = -1;

  public interface FragmentContext
  {
    public enum State
    {
      RUN, SUCEEDED, FAILED
    }
    State state();
    String queryId();
    ResponseContext responseContext();

    /**
     * Checks if a query timeout has occurred. If so, will throw
     * an unchecked exception. The operator need not catch this
     * exception: the fragment runner will unwind the stack and
     * call each operator's {@code close()} method on timeout.
     */
    void checkTimeout();
  }

  public static class FragmentContextImpl implements FragmentContext
  {
    private State state = State.RUN;
    private final ResponseContext responseContext;
    private final String queryId;
    private final long startTimeMillis;
    private final long timeoutMs;
    private final long timeoutAt;

    public FragmentContextImpl(
        final String queryId,
        long timeoutMs,
        final ResponseContext responseContext)
    {
      this.queryId = queryId;
      this.responseContext = responseContext;
      this.startTimeMillis = System.currentTimeMillis();
      this.timeoutMs = timeoutMs;
      if (timeoutMs < 0) {
        this.timeoutAt = 0;
      } else {
        this.timeoutAt = startTimeMillis + timeoutMs;
      }
    }

    @Override
    public State state() {
      return state;
    }

    @Override
    public String queryId() {
      return queryId;
    }

    @Override
    public ResponseContext responseContext() {
      return responseContext;
    }

    public void completed(boolean success)
    {
      state = success ? State.SUCEEDED : State.FAILED;
    }

    @Override
    public void checkTimeout() {
      if (timeoutAt > 0 && System.currentTimeMillis() >= timeoutAt) {
        throw new QueryTimeoutException(
            StringUtils.nonStrictFormat("Query [%s] timed out after [%d] ms",
                queryId, timeoutMs));
      }
    }

    protected void recordRunTime()
    {
      if (timeoutAt == 0) {
        return;
      }
      // This is very likely wrong
      responseContext.put(
          ResponseContext.Key.TIMEOUT_AT,
          timeoutAt - (System.currentTimeMillis() - startTimeMillis)
      );
    }
  }

  public static FragmentContext defaultContext() {
    return new FragmentContextImpl(
        "unknown",
        NO_TIMEOUT,
        ResponseContext.createEmpty());
  }

  /**
   * Variation of {@code BaseSequence}, but modified to capture exceptions,
   * and create the operator iterator directly.
   */
  private class FragmentSequence<T> implements Sequence<T>
  {
    @SuppressWarnings("unchecked")
    private Iterator<T> makeIter()
    {
      return (Iterator<T>) root().open(context);
    }

    @Override
    public <OutType> OutType accumulate(final OutType initValue, final Accumulator<OutType, T> fn)
    {
      Iterator<T> iterator = makeIter();
      OutType accumulated = initValue;

      try {
        while (iterator.hasNext()) {
          accumulated = fn.accumulate(accumulated, iterator.next());
        }
      }
      catch (Throwable t) {
        try {
          finish(t);
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        throw t;
      }
      finish(null);
      return accumulated;
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(
        final OutType initValue,
        final YieldingAccumulator<OutType, T> accumulator
    )
    {
      Iterator<T> iterator = makeIter();

      try {
        return makeYielder(initValue, accumulator, iterator);
      }
      catch (Throwable t) {
        try {
          finish(t);
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        throw t;
      }
    }

    private <OutType> Yielder<OutType> makeYielder(
        final OutType initValue,
        final YieldingAccumulator<OutType, T> accumulator,
        final Iterator<T> iter
    )
    {
      OutType retVal = initValue;
      while (!accumulator.yielded() && iter.hasNext()) {
        retVal = accumulator.accumulate(retVal, iter.next());
      }

      if (!accumulator.yielded()) {
        return Yielders.done(
            retVal,
            (Closeable) () -> finish(null)
        );
      }

      final OutType finalRetVal = retVal;
      return new Yielder<OutType>()
      {
        @Override
        public OutType get()
        {
          return finalRetVal;
        }

        @Override
        public Yielder<OutType> next(OutType initValue)
        {
          accumulator.reset();
          try {
            return makeYielder(initValue, accumulator, iter);
          }
          catch (Throwable t) {
            try {
              finish(t);
            }
            catch (Exception e) {
              t.addSuppressed(e);
            }
            throw t;
          }
        }

        @Override
        public boolean isDone()
        {
          return false;
        }

        @Override
        public void close()
        {
          finish(null);
        }
      };
    }
  }

  private final long timeoutMs;
  private final List<Operator> operators = new ArrayList<>();
  private FragmentContextImpl context;

  public FragmentRunner(long timeoutMs)
  {
    this.timeoutMs = timeoutMs;
  }

  public Operator add(Operator op)
  {
    operators.add(op);
    return op;
  }

  public Operator root()
  {
    Preconditions.checkState(!operators.isEmpty());
    return operators.get(operators.size() - 1);
  }

  public void start(FragmentContextImpl context)
  {
    this.context = context;
  }

  public <T> QueryRunner<T> toRunner()
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext) {
        start(new FragmentContextImpl(
            queryPlus.getQuery().getId(),
            timeoutMs,
            responseContext
            ));
        return new FragmentSequence<T>();
      }
    };
  }

  public void finish(Throwable t)
  {
    context.completed(t == null);
    close();
  }

  /**
   * Closes all operators from the leaves to the root.
   * As a result, operators must not call their children during
   * the {@code close()} call. Errors are collected, but all operators are closed
   * regardless of exceptions.
   */
  public void close()
  {
    List<Exception> exceptions = new ArrayList<>();
    for (Operator op : operators) {
      try {
        op.close(false);
      }
      catch (Exception e) {
        exceptions.add(e);
      }
    }
    // TODO: Do something with the exceptions
    context.recordRunTime();
  }
}
