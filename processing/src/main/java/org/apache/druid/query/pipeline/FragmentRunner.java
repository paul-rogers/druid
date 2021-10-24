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

import com.google.common.base.Preconditions;

import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
import org.apache.druid.query.pipeline.Operator.OperatorFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Mechanism to manage a DAG (typically a tree in Druid) of operators. The operators form
 * a data pipeline: data flows from leaves though internal nodes to the root. When the DAG
 * defines the entire pipeline on one node, it forms a fragment of a larger query which
 * typically runs on multiple nodes.
 * <p>
 * The fragment is defined via a parallel tree of operator definitions emitted by a planner.
 * The definitions are static, typically reflect aspects of the user's request and system
 * metadata, but know nothing about the actual rows. The definitions give rise (via
 * operator factories) to the actual stateful operator DAG which runs the pipeline.
 * <p>
 * The fragment runner starts the root operator, which cascades the start operation down
 * to its children in a manner appropriate to each operator. (An operator that defers a
 * call to its children must ensure start is eventually called before reading data.)
 * <p>
 * The fragment runner provides closes all children from the bottom up on shut down.
 * Children that have previously been closed by their parents can ignore this call.
 * <p>
 * When building the operator DAG, the runner creates the leaves first, then passes
 * these as children to the next layer, and so on up to the root.
 */
public class FragmentRunner
{
  /**
   * Registry, typically global, but often ad-hoc for tests, which maps from operator
   * definition classes to the corresponding operator factory.
   */
  public static class OperatorRegistry
  {
    private final Map<Class<? extends OperatorDefn>, OperatorFactory> factories = new IdentityHashMap<>();

    public void register(Class<? extends OperatorDefn> defClass, OperatorFactory factory)
    {
      Preconditions.checkState(!factories.containsKey(defClass));
      factories.put(defClass, factory);
    }

    public OperatorFactory factory(OperatorDefn defn)
    {
      return Preconditions.checkNotNull(factories.get(defn.getClass()));
    }
  }

  /**
   * The consumer is a class which accepts each row produced by the runner
   * and does something with it. The consumer returns <code>true</code> if
   * it wants more rows, </code>false</code> if it wants to terminate
   * results early.
   *
   * @param <T>
   */
  public interface Consumer
  {
    boolean accept(Object row);
  }

  public interface FragmentContext
  {
    ResponseContext responseContext();
  }

  public static FragmentContext defaultContext() {
    ResponseContext context = ResponseContext.createEmpty();
    return new FragmentContext() {
      @Override
      public ResponseContext responseContext() {
        return context;
      }
    };
  }

  private final OperatorRegistry registry;
  private final FragmentContext context;
  private final List<Operator> operators = new ArrayList<>();

  public FragmentRunner(OperatorRegistry registry, FragmentContext context)
  {
    this.registry = registry;
    this.context = context;
  }

  public Operator build(OperatorDefn rootDefn)
  {
    List<Operator> children = new ArrayList<>();
    for (OperatorDefn child : rootDefn.children()) {
      children.add(build(child));
    }
    OperatorFactory factory = registry.factory(rootDefn);
    Operator rootOp = factory.build(rootDefn, children, context);
    operators.add(rootOp);
    return rootOp;
  }

  public Operator root() {
    if (operators.isEmpty()) {
      return null;
    }
    return operators.get(operators.size() - 1);
  }

  public void run(Consumer consumer) {
   Operator root = root();
   root.start();
   for (Object row : Operators.toIterable(root)) {
      if (!consumer.accept(row)) {
        break;
      }
    }
  }

  public void close()
  {
    close(operators);
  }

  public void fullRun(Consumer consumer)
  {
    run(consumer);
    close();
  }

  private void close(List<Operator> ops)
  {
    List<Exception> exceptions = new ArrayList<>();
    for (Operator op : ops) {
      try {
        op.close(false);
      }
      catch (Exception e) {
        exceptions.add(e);
      }
    }
  }
}
