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

import org.apache.druid.exec.fragment.FragmentProfile.OperatorChildNode;
import org.apache.druid.exec.fragment.FragmentProfile.OperatorNode;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Map.Entry;

/**
 * Visualize an operator plan in an Impala-like text format.
 */
public class ProfileVisualizer
{
  private static final String INDENT = "| ";
  private static final String METRIC_INDENT = "  ";

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
       .append("Query ID:    ")
       .append(profile.queryId)
       .append("\nSlice ID:    ")
       .append(profile.sliceId)
       .append("\nFragment ID: ")
       .append(profile.fragmentId)
       .append("\nRuntime (ms): ")
       .append(profile.runTimeMs)
       .append("\n");
    if (profile.error != null) {
      buf.append("Error: ")
         .append(profile.error.getClass().getSimpleName())
         .append(" - ")
         .append(profile.error.getMessage())
         .append("\n");
    }
    if (profile.root == null) {
      buf.append("Fragment has no operators!\n");
    } else {
      renderNode(0, profile.root);
    }

    return buf.toString();
  }

  private void renderNode(int level, OperatorNode node)
  {
    if (node.profile.omitFromProfile &&
        node.children != null &&
        node.children.size() == 1) {
      renderOperatorChild(level, node.children.get(0));
      return;
    }
    String indent = StringUtils.repeat(INDENT, level);
    buf.append(indent)
       .append(node.id)
       .append(": ")
       .append(node.profile.operatorName)
       .append("\n");
    int childCount = node.children == null ? 0 : node.children.size();
    String innerIndent = indent + StringUtils.repeat(INDENT, childCount);
    for (Entry<String, Object> entry : node.profile.metrics().entrySet()) {
      buf.append(innerIndent)
         .append(METRIC_INDENT)
         .append(entry.getKey())
         .append(": ")
         .append(entry.getValue())
         .append("\n");
    }
    if (isLeaf(node)) {
      return;
    }
    buf.append(innerIndent).append("\n");
    for (int i = childCount - 1; i >= 0; i--) {
      renderOperatorChild(level + i, node.children.get(i));
    }
  }

  private void renderOperatorChild(int level, OperatorChildNode child)
  {
    if (child.operator == null) {
      String indent = StringUtils.repeat(INDENT, level);
      buf.append(indent)
         .append("Slice ")
         .append(child.slice)
         .append("\n");
    } else {
      renderNode(level, child.operator);
    }
  }

  private boolean isLeaf(OperatorNode node)
  {
    if (node.children == null || node.children.isEmpty()) {
      return true;
    }
    if (node.children.size() > 1) {
      return false;
    }
    OperatorChildNode child = node.children.get(0);
    if (child.operator == null) {
      return false;
    }
    return child.operator.profile.omitFromProfile && isLeaf(child.operator);
  }
}
