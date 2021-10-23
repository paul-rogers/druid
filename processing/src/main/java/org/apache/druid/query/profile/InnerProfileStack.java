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

package org.apache.druid.query.profile;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.List;

/**
 * A profile stack for a subtree: usually for a sequence that
 * incrementally creates child profiles after the original
 * {@code QueryRunner.run()} call completes. By that time, the
 * original full stack in the response context has been popped
 * and the desired parent is no longer available. Instead, the sequence
 * should hold onto a substack rooted on the desired parent.
 * <p>
 * A subtree does not support the idea of a root operator since the
 * root is somewhere in the profile tree above this subtree.
 */
public class InnerProfileStack implements ProfileStack
{
  private final List<OperatorProfile> stack = new ArrayList<>();

  public InnerProfileStack(OperatorProfile localRoot)
  {
    stack.add(localRoot);
  }

  @Override
  public void push(OperatorProfile profile)
  {
    leaf(profile);
    restore(profile);
  }

  @Override
  public void pop(OperatorProfile profile)
  {
    Preconditions.checkState(stack.size() > 1);
    Preconditions.checkState(stack.remove(stack.size() - 1) == profile);
  }

  @Override
  public OperatorProfile root()
  {
    throw new ISE("Not supported in a subtree");
  }

  @Override
  public void leaf(OperatorProfile profile)
  {
    stack.get(stack.size() - 1).addChild(profile);
  }

  @Override
  public void wrapRoot(OperatorProfile profile)
  {
    throw new ISE("Not supported in a subtree");
  }

  @Override
  public void restore(OperatorProfile profile)
  {
    stack.add(profile);
  }
}
