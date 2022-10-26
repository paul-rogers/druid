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
import org.apache.druid.exec.plan.FragmentSpec;
import org.apache.druid.exec.plan.OperatorSpec;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a physical plan into a set of operators to execute that plan.
 */
public class FragmentConverter
{
  private final OperatorConverter opConverter;
  private final FragmentManager fragment;
  private final FragmentPlan plan;

  public FragmentConverter(OperatorConverter opConverter, FragmentManager fragment)
  {
    this.opConverter = opConverter;
    this.fragment = fragment;
    this.plan = fragment.plan();
  }

  public static FragmentManager build(OperatorConverter opConverter, String queryId, FragmentSpec fragSpec)
  {
    FragmentManager fragment = new FragmentManager(queryId, fragSpec);
    build(opConverter, fragment);
    return fragment;
  }

  public static void build(OperatorConverter opConverter, FragmentManager fragment)
  {
    FragmentConverter builder = new FragmentConverter(opConverter, fragment);
    builder.createTree();
  }

  public Operator<?> createTree()
  {
    Operator<?> root = createSubtree(plan.operator(plan.spec().rootId()));
    fragment.registerRoot(root);
    return root;
  }

  private Operator<?> createSubtree(OperatorSpec opSpec)
  {
    List<Operator<?>> children = opSpec.children().stream()
        .map(childId -> plan.operator(childId))
        .map(child -> createSubtree(child))
        .collect(Collectors.toList());
    Operator<?> op = opConverter.create(fragment, opSpec, children);
    fragment.register(opSpec, op);
    return op;
  }
}
