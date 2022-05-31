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
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentContext.State;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;

import java.util.Iterator;
import java.util.List;

/**
 * Constructs a fragment by registering a set of operators. Used during the
 * transition when query runners create operators dynamically.
 *
 * Also provides an API to start the fragment running either by using
 * a sequence or a root operator to fetch rows.
 */
public class FragmentBuilderImpl implements FragmentBuilder
{
  private final FragmentContextImpl context;

  public FragmentBuilderImpl(
      final String queryId,
      long timeoutMs,
      final ResponseContext responseContext)
  {
    this.context = new FragmentContextImpl(queryId, timeoutMs, responseContext);
  }

  /**
   * Adds an operator to the list of operators to close. Assumes operators are
   * added bottom-up (as is required so that operators are given their inputs)
   * so that the last operator in the list is the root we want to execute.
   */
  @Override
  public void register(Operator<?> op)
  {
    Preconditions.checkState(context.state == State.START);
    context.operators.add(op);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ResultIterator<T> open()
  {
    if (context.state != State.START) {
      throw new ISE("Fragment is already opened()");
    }
    Operator<T> rootOp;
    if (context.operators.size() == 0) {
      // Rather silly "DAG": does nothing.
      rootOp = new NullOperator<T>(this);
    } else {
      rootOp = (Operator<T>) context.operators.get(context.operators.size() - 1);
    }
    return open(rootOp);
  }

  @Override
  public <T> ResultIterator<T> open(Operator<T> rootOp)
  {
    return new ResultIteratorImpl<T>(context, rootOp);
  }

  @Override
  public <T> Sequence<T> toSequence()
  {
    return toSequence(open());
  }

  @Override
  public <T> Sequence<T> toSequence(Operator<T> rootOp)
  {
    return toSequence(open(rootOp));
  }

  private <T> Sequence<T> toSequence(ResultIterator<T> rootOp)
  {
    return new BaseSequence<T, Iterator<T>>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return rootOp;
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            rootOp.close();
          }
        }
    );
  }

  @Override
  public <T> List<T> toList()
  {
    return toList(open());
  }

  @Override
  public <T> List<T> toList(Operator<T> rootOp)
  {
    return toList(open(rootOp));
  }

  private <T> List<T> toList(ResultIterator<T> rootOp)
  {
    try {
      return Lists.newArrayList(rootOp);
    }
    finally {
      rootOp.close();
    }
  }

  @Override
  public FragmentContext context()
  {
    return context;
  }
}
