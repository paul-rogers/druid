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

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.ResponseContext;

public class FragmentContextImpl implements FragmentContext
{
  private final long timeoutMs;
  private final List<Operator> operators = new ArrayList<>();
  private State state = State.RUN;
  private final ResponseContext responseContext;
  private final String queryId;
  private final long startTimeMillis;
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

  @Override
  public void register(Operator op)
  {
    operators.add(op);
  }

  /**
   * Closes all operators from the leaves to the root.
   * As a result, operators must not call their children during
   * the {@code close()} call. Errors are collected, but all operators are closed
   * regardless of exceptions.
   */
  @Override
  public void close(boolean succeeded)
  {
    if (state != State.RUN) {
      return;
    }
    state = succeeded ? State.SUCEEDED : State.FAILED;
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
    recordRunTime();
  }
}