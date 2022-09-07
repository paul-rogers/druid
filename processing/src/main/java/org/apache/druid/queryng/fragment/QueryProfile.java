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

import org.apache.druid.queryng.fragment.FragmentManager.OperatorTracker;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class QueryProfile
{
  public static class SliceNode
  {
    public final int sliceId;
    public final List<FragmentNode> fragments;

    public SliceNode(int sliceId, List<FragmentNode> fragments)
    {
      super();
      this.sliceId = sliceId;
      this.fragments = fragments;
    }
  }

  /**
   * Profile for an operator fragment with operators organized into a tree.
   */
  public static class FragmentNode
  {
    public final String queryId;
    public final int fragmentId;
    public final long runTimeMs;
    public final Exception error;
    public final List<OperatorNode> roots;

    protected FragmentNode(
        FragmentManager fragment,
        int fragmentId,
        final List<OperatorNode> roots
    )
    {
      this.fragmentId = fragmentId;
      this.queryId = fragment.queryId();
      this.runTimeMs = fragment.elapsedTimeMs();
      this.error = fragment.exception();
      this.roots = roots;
    }
  }

  public static class OperatorNode
  {
    public final OperatorProfile profile;
    public final List<OperatorNode> children;

    protected OperatorNode(
        final OperatorProfile operatorProfile,
        final List<OperatorNode> children)
    {
      this.profile = operatorProfile;
      this.children = children == null || children.isEmpty() ? null : children;
    }
  }

  private static class Builder
  {
    private final QueryManager query;
    private Exception error;
    private SliceNode rootSlice;

    public Builder(QueryManager query)
    {
      this.query = query;
    }

    public QueryProfile build()
    {
      error = query.rootFragment().exception();
      rootSlice = profileRootFragment(query.rootFragment());
      return new QueryProfile(this);
    }

    public SliceNode profileRootFragment(FragmentManager fragment)
    {
      Map<Operator<?>, OperatorTracker> operators = fragment.operators();
      Map<Operator<?>, Boolean> rootCandidates = new IdentityHashMap<>();
      for (Operator<?> op : operators.keySet()) {
        rootCandidates.put(op, true);
      }
      for (Entry<Operator<?>, OperatorTracker> entry : operators.entrySet()) {
        for (Operator<?> child : entry.getValue().children) {
          rootCandidates.put(child, false);
        }
      }
      List<OperatorNode> rootProfiles = new ArrayList<>();
      Operator<?> root = fragment.topMostOperator();
      if (root != null) {
        rootCandidates.put(root, false);
        rootProfiles.add(profileOperator(operators, root));
      }
      for (Entry<Operator<?>, Boolean> entry : rootCandidates.entrySet()) {
        if (entry.getValue()) {
          rootProfiles.add(profileOperator(operators, entry.getKey()));
        }
      }
      return new SliceNode(
          1,
          Collections.singletonList(new FragmentNode(fragment, 1, rootProfiles))
      );
    }

    private OperatorNode profileOperator(
        Map<Operator<?>, OperatorTracker> operators,
        Operator<?> root
    )
    {
      List<OperatorNode> childProfiles;
      OperatorProfile rootProfile;
      OperatorTracker tracker = operators.get(root);
      if (tracker == null) {
        childProfiles = null;
        rootProfile = null;
      } else {
        childProfiles = new ArrayList<>();
        for (Operator<?> child : tracker.children) {
          childProfiles.add(profileOperator(operators, child));
        }
        rootProfile = tracker.profile;
      }
       if (rootProfile == null) {
        rootProfile = new OperatorProfile(root.getClass().getSimpleName());
      }
      return new OperatorNode(
          rootProfile,
          childProfiles
      );
    }
  }

  public final String queryId;
  public final long runTimeMs;
  public final Exception error;
  public final SliceNode rootSlice;

  public static QueryProfile build(QueryManager query) {
    return new Builder(query).build();
  }

  private QueryProfile(Builder builder)
  {
    this.queryId = builder.query.queryId();
    this.runTimeMs = builder.query.runTimeMs();
    this.error = builder.error;
    this.rootSlice = builder.rootSlice;
  }
}
