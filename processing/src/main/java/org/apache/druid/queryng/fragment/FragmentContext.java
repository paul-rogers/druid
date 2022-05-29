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

import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.operators.Operator;

/**
 * Manages the operators within a fragment and provides fragment-level context.
 */
public interface FragmentContext
{
  long NO_TIMEOUT = -1;

  enum State
  {
    RUN, SUCEEDED, FAILED
  }

  /**
   * Add an operator dynamically. The operator will be closed
   * at query completion.
   */
  void register(Operator op);
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

  void missingSegment(SegmentDescriptor descriptor);

  /**
   * Closes all registered operators. To be called only at the root
   * level by the mechanism that manages the overall query fragment.
   */
  void close(boolean succeeded);

  static FragmentContext defaultContext()
  {
    return new FragmentContextImpl(
        "unknown",
        NO_TIMEOUT,
        ResponseContext.createEmpty());
  }
}
