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

import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;

public class Operators
{
  public static final String ENABLE_SCAN_OPERATOR = "useScanV2";

  public static boolean isEnabled(Query<?> query, String flag) {
    return query.getContextBoolean(flag, false);
  }

  /**
   * Convenience function to open the operator and return its
   * iterator as an {@code Iterable}.
   */
  public static Iterable<Object> toIterable(Operator op, FragmentContext context) {
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
    }

    public Operator unwrap()
    {
      return op;
    }
  }

  /**
   * Converts a stand-alone operator to a sequence outside the context of a fragment
   * runner. The sequence starts and closes the operator.
   */
  public static <T> Sequence<T> toSequence(Operator op, FragmentContext context) {
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

  public static <T> QueryRunnerOperator<T> toOperator(QueryRunner<T> runner, QueryPlus<T> query) {
    return new QueryRunnerOperator<T>(runner, query);
  }
}
