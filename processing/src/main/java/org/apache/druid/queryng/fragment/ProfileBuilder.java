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

import org.apache.druid.queryng.fragment.FragmentProfile.ProfileNode;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ProfileBuilder
{
  private long runTimeMs;
  private Exception error;
  private Operator<?> root;
  private final Map<Operator<?>, List<Operator<?>>> relationships = new IdentityHashMap<>();
  private final Map<Operator<?>, OperatorProfile> profiles = new IdentityHashMap<>();

  public void registerRoot(Operator<?> root)
  {
    this.root = root;
  }

  public void registerChild(Operator<?> parent, Operator<?> child)
  {
    List<Operator<?>> children = relationships.computeIfAbsent(parent, k -> new ArrayList<>());
    children.add(child);
  }

  public synchronized void updateProfile(Operator<?> op, OperatorProfile profile)
  {
    profiles.put(op, profile);
  }

  public FragmentProfile build(FragmentContextImpl context)
  {
    Collection<Operator<?>> operators = context.operators();
    Map<Operator<?>, Boolean> rootCandidates = new IdentityHashMap<>();
    for (Operator<?> op : operators) {
      rootCandidates.put(op, true);
    }
    for (Entry<Operator<?>, List<Operator<?>>> entry : relationships.entrySet()) {
      for (Operator<?> child : entry.getValue()) {
        rootCandidates.put(child, false);
      }
    }
    List<ProfileNode> rootProfiles = new ArrayList<>();
    if (root != null) {
      rootCandidates.put(root, false);
      rootProfiles.add(buildProfile(root));
    }
    for (Entry<Operator<?>, Boolean> entry : rootCandidates.entrySet()) {
      if (entry.getValue()) {
        rootProfiles.add(buildProfile(entry.getKey()));
      }
    }
    return new FragmentProfile(context, rootProfiles);
  }

  private ProfileNode buildProfile(Operator<?> root)
  {
    List<ProfileNode> childProfiles;
    List<Operator<?>> children = relationships.get(root);
    if (children == null) {
      childProfiles = null;
    } else {
      childProfiles = new ArrayList<>();
      for (Operator<?> child : children) {
        childProfiles.add(buildProfile(child));
      }
    }
    OperatorProfile rootProfile = profiles.get(root);
    if (rootProfile == null) {
      rootProfile = new OperatorProfile(root.getClass().getSimpleName());
    }
    return new ProfileNode(
        rootProfile,
        childProfiles
    );
  }
}
