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

package org.apache.druid.queryng.operators;

import org.apache.druid.java.util.common.guava.Accumulators;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.general.QueryRunnerOperator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Utility functions related to operators.
 */
public class Operators
{
  public static final String CONTEXT_VAR = "queryng";

  /**
   * Determine if the Query NG (operator-based) engine is enabled for the
   * given query (given as a QueryPlus). Query NG is enabled if the QueryPlus
   * includes the fragment context needed by the Query NG engine.
   */
  public static boolean enabledFor(final QueryPlus<?> queryPlus)
  {
    return queryPlus.fragmentContext() != null;
  }

  /**
   * Determine if Query NG should be enabled for the given query;
   * that is, if the query should have a fragment context attached.
   * At present, Query NG is enabled if the query is a scan query and
   * the query has the "queryng" context variable set. The caller
   * should already have checked if the Query NG engine is enabled
   * globally. If Query NG is enabled for a query, then the caller
   * will attach a fragment context to the query's QueryPlus.
   */
  public static boolean isEnabled(Query<?> query)
  {
    // Query has to be of the currently-supported type
    if (!(query instanceof ScanQuery)) {
      return false;
    }
    return query.getContextBoolean(CONTEXT_VAR, false);
  }

  /**
   * Convenience function to open the operator and return its
   * iterator as an {@code Iterable}.
   */
  public static Iterable<Object> toIterable(Operator op, FragmentContext context)
  {
    return new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator()
      {
        return op.open(context);
      }
    };
  }

  /**
   * Wraps an operator in a sequence using the standard base sequence
   * iterator mechanism (since an operator looks like an iterator.)
   *
   * This is a named class so we can unwrap the operator in
   * {@link #runToProducer()} below.
   */
  public static class OperatorWrapperSequence<T> extends BaseSequence<T, Iterator<T>>
  {
    private final Operator op;
    private final FragmentContext context;

    public OperatorWrapperSequence(Operator op, FragmentContext context)
    {
      super(new BaseSequence.IteratorMaker<T, Iterator<T>>()
      {
        @SuppressWarnings("unchecked")
        @Override
        public Iterator<T> make()
        {
          return (Iterator<T>) op.open(context);
        }

        @Override
        public void cleanup(Iterator<T> iterFromMake)
        {
          op.close(true);
        }
      });
      this.op = op;
      this.context = context;
    }

    public Operator unwrap()
    {
      return op;
    }

    /**
     * This will materialize the entire sequence from the wrapped
     * operator.  Use at your own risk.
     */
    @Override
    public List<T> toList()
    {
      return Operators.toList(op, context);
    }
  }

  /**
   * Converts a stand-alone operator to a sequence outside the context of a fragment
   * runner. The sequence starts and closes the operator.
   */
  public static <T> Sequence<T> toSequence(Operator op, FragmentContext context)
  {
    return new OperatorWrapperSequence<>(op, context);
  }

  /**
   * Wrap a sequence in an operator.
   * <p>
   * If the input sequence is a wrapper around an operator, then
   * (clumsily) unwraps that operator and returns it directly.
   */
  public static Operator toOperator(Sequence<?> sequence)
  {
    if (sequence instanceof OperatorWrapperSequence) {
      return ((OperatorWrapperSequence<?>) sequence).unwrap();
    }
    return new SequenceOperator(sequence);
  }

  /**
   * Create an operator which wraps a query runner which allows a query runner
   * to be an input to an operator. The runner, and its sequence, will be optimized
   * away at runtime if both the upstream and downstream items are both operators,
   * but the shim is left in place if the upstream is actually a query runner.
   */
  public static <T> QueryRunnerOperator<T> toOperator(QueryRunner<T> runner, QueryPlus<T> query)
  {
    return new QueryRunnerOperator<T>(runner, query);
  }

  /**
   * This will materialize the entire sequence from the wrapped
   * operator.  Use at your own risk.
   */
  @SuppressWarnings("unchecked")
  public static <T> List<T> toList(Operator op, FragmentContext context)
  {
    List<T> results = new ArrayList<>();
    Iterator<Object> iter = op.open(context);
    while (iter.hasNext()) {
      results.add((T) iter.next());
    }
    op.close(true);
    return results;
  }
}
