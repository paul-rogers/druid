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

package org.apache.druid.exec.factory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.exec.fragment.FragmentManager;
import org.apache.druid.exec.internalSort.InternalSortFactory;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorFactory;
import org.apache.druid.exec.plan.OperatorSpec;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Converts a physical plan into a set of operators to execute that plan.
 */
public class OperatorConverter
{
  private final Map<Class<? extends OperatorSpec>, OperatorFactory> factories;

  public OperatorConverter()
  {
    this(Collections.emptyList());
  }

  public OperatorConverter(List<OperatorFactory> extns)
  {
    List<OperatorFactory> stdOps = Collections.singletonList(new InternalSortFactory());
    ImmutableMap.Builder<Class<? extends OperatorSpec>, OperatorFactory> builder = ImmutableMap.builder();
    for (OperatorFactory factory : Iterables.concat(stdOps, extns)) {
      builder.put(factory.accepts(), factory);
    }
    factories = builder.build();
  }

  public Operator create(FragmentManager context, OperatorSpec plan, List<Operator> children)
  {
    OperatorFactory factory = factories.get(plan.getClass());
    if (factory == null) {
      throw new ISE("Operator plan %s has no registered factory", plan.getClass().getSimpleName());
    }
    Operator op = factory.create(context, plan, children);
    context.register(plan, op);
    return op;
  }

  public Operator createTree(FragmentManager context, OperatorSpec plan)
  {
    Operator root = createSubtree(context, plan);
    context.registerRoot(root);
    return root;
  }

  private Operator createSubtree(FragmentManager context, OperatorSpec plan)
  {
    List<OperatorSpec> childPlans = plan.children();
    List<Operator> children = new ArrayList<>();
    for (OperatorSpec childPlan : childPlans) {
      children.add(createSubtree(context, childPlan));
    }
    return create(context, plan, children);
  }
}
