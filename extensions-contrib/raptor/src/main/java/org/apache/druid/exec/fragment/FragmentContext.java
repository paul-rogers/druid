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

import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorProfile;
import org.apache.druid.math.expr.ExprMacroTable;

/**
 * Provides fragment-level context to operators within a single
 * fragment.
 * <p>
 * Passed around while building a DAG of operators:
 * provides access to the {@link FragmentContext} which each operator
 * may use, and a method to register operators with the fragment.
 */
public interface FragmentContext
{
  @SuppressWarnings("unused") // Needed in the future
  long NO_TIMEOUT = -1;

  enum State
  {
    START, RUN, FAILED, CLOSED
  }

  /**
   * State of the fragment. Generally of interest to the top-level fragment
   * runner. A fragment enters the run state when it starts running, the
   * failed state when the top-level runner calls {@link #failed(), and
   * the closed state after a successful run.
   */
  State state();

  /**
   * The native query ID associated with the native query that gave rise
   * to the fragment's operator DAG. There is one query ID per fragment.
   * Nested queries provide a separate slice (which executes as a fragment)
   * for each of the subqueries.
   */
  String queryId();

  /**
   * Checks if a query timeout has occurred. If so, will throw
   * an unchecked exception. The operator need not catch this
   * exception: the fragment runner will unwind the stack and
   * call each operator's {@code close()} method on timeout.
   */
  void checkTimeout();

  /**
   * Mark the fragment as failed. Generally set at the top of the operator
   * DAG in response to an exception. Operators just throw an exception to
   * indicate failure.
   */
  boolean failed();

  /**
   * Provide an update of the profile (statistics) for a specific
   * operator. Can be done while the fragment runs, or at close time.
   * The last update wins, so metrics must be cumulative.
   */
  void updateProfile(Operator<?> op, OperatorProfile profile);

  ExprMacroTable macroTable();
}
