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
import org.apache.druid.exec.plan.OperatorSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.ArrayList;
import java.util.List;

public class FragmentProfile
{
  protected static final EmittingLogger log = new EmittingLogger(FragmentProfile.class);

  public static class OperatorNode
  {
    public final int id;
    public final OperatorProfile profile;
    public final List<OperatorChildNode> children;

    protected OperatorNode(
        final int id,
        final OperatorProfile operatorProfile,
        final List<OperatorChildNode> children)
    {
      this.id = id;
      this.profile = operatorProfile;
      this.children = children == null || children.isEmpty() ? null : children;
    }
  }

  public static class OperatorChildNode
  {
    public final OperatorNode operator;
    public final int slice;

    public OperatorChildNode(OperatorNode operator, int slice)
    {
      super();
      this.operator = operator;
      this.slice = slice;
    }
  }

  public static FragmentProfile profile(FragmentManager fragment)
  {
    OperatorNode root = profileOperator(fragment, fragment.plan().spec().rootId());
    return new FragmentProfile(fragment, root);
  }

  private static OperatorNode profileOperator(
      FragmentManager fragment,
      int rootId
  )
  {
    Operator root = fragment.operator(rootId);
    List<OperatorChildNode> childProfiles = new ArrayList<>();
    OperatorProfile rootProfile = fragment.profile(root);
    OperatorSpec opSpec = fragment.plan().operator(rootId);
    childProfiles = new ArrayList<>();
    for (Integer childId : opSpec.children()) {
      childProfiles.add(new OperatorChildNode(profileOperator(fragment, childId), 0));
    }
    if (rootProfile == null) {
      rootProfile = new OperatorProfile(root.getClass().getSimpleName());
    }
    return new OperatorNode(
        rootId,
        rootProfile,
        childProfiles
    );
  }

  public final String queryId;
  public final int sliceId;
  public final int fragmentId;
  public final long runTimeMs;
  public final Exception error;
  public final OperatorNode root;

  protected FragmentProfile(
      final FragmentManager fragment,
      final OperatorNode root
  )
  {
    this.sliceId = fragment.plan().spec().sliceId();
    this.fragmentId = fragment.plan().spec().fragmentId();
    this.queryId = fragment.queryId();
    this.runTimeMs = fragment.elapsedTimeMs();
    this.error = fragment.exception();
    this.root = root;
  }
}
