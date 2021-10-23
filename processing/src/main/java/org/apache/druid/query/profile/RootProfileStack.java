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
import org.apache.druid.query.profile.OperatorProfile.OpaqueOperatorProfile;

import java.util.ArrayList;
import java.util.List;

/**
 * Profile stack for the root of the operator tree (as seen by the
 * fragment). Keeps track of the operator stack and the root
 * operator.
 */
public class RootProfileStack implements ProfileStack
{
  private final List<OperatorProfile> stack = new ArrayList<>();
  private OperatorProfile root;

  @Override
  public void push(OperatorProfile profile)
  {
    leaf(profile);
    restore(profile);
  }

  @Override
  public void pop(OperatorProfile profile)
  {
    Preconditions.checkState(!stack.isEmpty());
    Preconditions.checkState(stack.remove(stack.size() - 1) == profile);
  }

  @Override
  public OperatorProfile root()
  {
    Preconditions.checkState(stack.isEmpty());
    if (root == null) {
      root = new OpaqueOperatorProfile();
    }
    return root;
  }

  @Override
  public void leaf(OperatorProfile profile)
  {
    if (stack.isEmpty()) {
      Preconditions.checkState(root == null);
      root = profile;
    } else {
      stack.get(stack.size() - 1).addChild(profile);
    }
  }

  @Override
  public void wrapRoot(OperatorProfile profile)
  {
    profile.addChild(root());
    root = profile;
  }

  @Override
  public void restore(OperatorProfile profile)
  {
    stack.add(profile);
  }
}
