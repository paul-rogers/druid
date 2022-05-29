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

package org.apache.druid.queryng.fragment;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class FragmentContextImpl implements FragmentContext
{
  private final long timeoutMs;
  private final List<Operator<?>> operators = new ArrayList<>();
  private final ResponseContext responseContext;
  private final String queryId;
  private final long startTimeMillis;
  private long closeTimeMillis;
  private final long timeoutAt;
  protected State state = State.START;
  private List<Exception> exceptions = new ArrayList<>();
  private final List<Consumer<FragmentContext>> closeListeners = new ArrayList<>();
  private final ProfileBuilder profileBuilder = new ProfileBuilder();

  protected FragmentContextImpl(
      final String queryId,
      long timeoutMs,
      final ResponseContext responseContext)
  {
    this.queryId = queryId;
    this.responseContext = responseContext;
    this.startTimeMillis = System.currentTimeMillis();
    this.timeoutMs = timeoutMs;
    if (timeoutMs > 0) {
      this.timeoutAt = startTimeMillis + timeoutMs;
    } else {
      this.timeoutAt = JodaUtils.MAX_INSTANT;
    }
  }

  @Override
  public State state()
  {
    return state;
  }

  @Override
  public synchronized void register(Operator<?> op)
  {
    Preconditions.checkState(state == State.START || state == State.RUN);
    operators.add(op);
  }

  @Override
  public synchronized void registerChild(Operator<?> parent, Operator<?> child)
  {
    Preconditions.checkState(state == State.START || state == State.RUN);
    profileBuilder.registerChild(parent, child);
  }

  @Override
  public Exception exception()
  {
    if (exceptions.isEmpty()) {
      return null;
    } else {
      return exceptions.get(0);
    }
  }

  @Override
  public String queryId()
  {
    return queryId;
  }

  @Override
  public ResponseContext responseContext()
  {
    return responseContext;
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

  protected void recordRunTime()
  {
    if (timeoutAt == 0) {
      return;
    }
    // This is very likely wrong
    responseContext.put(
        ResponseContext.Keys.TIMEOUT_AT,
        timeoutAt - (System.currentTimeMillis() - startTimeMillis)
    );
  }

  @Override
  public void missingSegment(SegmentDescriptor descriptor)
  {
    responseContext.add(
        ResponseContext.Keys.MISSING_SEGMENTS,
        Collections.singletonList(descriptor)
    );
  }

  /**
   * Closes all operators from the leaves to the root.
   * As a result, operators must not call their children during
   * the {@code close()} call. Errors are collected, but all operators are closed
   * regardless of exceptions.
   */
  protected void close()
  {
    if (state == State.START) {
      state = State.CLOSED;
    }
    if (state == State.CLOSED) {
      return;
    }
    for (Operator<?> op : operators) {
      try {
        op.close(false);
      }
      catch (Exception e) {
        exceptions.add(e);
      }
    }
    state = State.CLOSED;
    closeTimeMillis = System.currentTimeMillis();
    for (Consumer<FragmentContext> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public synchronized void updateProfile(Operator<?> op, OperatorProfile profile)
  {
    profileBuilder.updateProfile(op, profile);
  }

  protected ProfileBuilder profileBuilder()
  {
    return profileBuilder;
  }

  public FragmentProfile buildProfile()
  {
    return profileBuilder.build(this);
  }

  @Override
  public void onClose(Consumer<FragmentContext> listener)
  {
    closeListeners.add(listener);
  }

  public Collection<Operator<?>> operators()
  {
    return operators;
  }

  public long elapsedTimeMs()
  {
    return closeTimeMillis - startTimeMillis;
  }
}
