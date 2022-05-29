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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.queryng.fragment.FragmentProfile.ProfileNode;

import java.util.List;
import java.util.Map.Entry;

/**
 * Visualize an operator plan in an Impala-like text format.
 */
public class ProfileVisualizer
{
  private final String INDENT = "| ";
  private final String METRIC_INDENT = "  ";

  private final FragmentProfile profile;
  private final StringBuilder buf = new StringBuilder();

  public ProfileVisualizer(FragmentProfile profile)
  {
    this.profile = profile;
  }

  public String render()
  {
    buf.setLength(0);
    buf.append("----------\n")
       .append("Query ID: ")
       .append(profile.queryId)
       .append("\n")
       .append("Runtime (ms): ")
       .append(profile.runTimeMs)
       .append("\n");
    if (profile.error != null) {
      buf.append("Error: ")
         .append(profile.error.getClass().getSimpleName())
         .append(" - ")
         .append(profile.error.getMessage())
         .append("\n");
    }
    buf.append("\n");
    for (ProfileNode root : profile.roots) {
      renderNode(0, root);
    }
    return buf.toString();
  }

  private void renderNode(int level, ProfileNode node)
  {
    if (node.profile.omitFromProfile && node.children.size() == 1) {
      renderNode(level, node.children.get(0));
      return;
    }
    String indent = StringUtils.repeat(INDENT, level);
    buf.append(indent)
       .append(node.profile.operatorName)
       .append("\n");
    List<ProfileNode> children = node.children;
    int childCount = children == null ? 0 : children.size();
    String innerIndent = indent + StringUtils.repeat(INDENT, childCount);
    for (Entry<String, Long> entry : node.profile.metrics().entrySet()) {
      buf.append(innerIndent)
         .append(METRIC_INDENT)
         .append(entry.getKey())
         .append(": ")
         .append(entry.getValue())
         .append("\n");
    }
    if (children == null) {
      return;
    }
    buf.append(innerIndent).append("\n");
    for (int i = childCount - 1; i >= 0; i--) {
      renderNode(level + i, children.get(i));
    }
  }
}
