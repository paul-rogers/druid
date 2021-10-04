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

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Represents a slice: a query distributed across a set of data nodes.
 * Each instance of a slice is a "fragment." This structure assumes that
 * every query is distributed to each data node once, possibly for many
 * segments.
 */
public class SliceNode
{
  private final String queryId;
  /**
   * Map of host names to fragments. Host names include the
   * host and port so that they are unique.
   */
  private final Map<String, FragmentNode> fragments;
  
  public SliceNode(String queryId)
  {
    // Merging is easier if we have an actual key, so if we
    // have no query ID, make one up: anonymous
    this.queryId = queryId == null ? "anonymous" : queryId;
    this.fragments = new HashMap<>();
  }
  
  public SliceNode(
      @JsonProperty("queryId") @Nullable String queryId,
      @JsonProperty("fragments") @Nullable Map<String, FragmentNode> fragments
  )
  {
    this.queryId = queryId;
    this.fragments = fragments == null ? new HashMap<>() : fragments;
  }

  @JsonProperty
  public String getQueryId()
  {
    return queryId;
  }
  
  @JsonProperty
  public Map<String, FragmentNode> getFragments()
  {
    return fragments;
  }

  public void add(FragmentNode fragment) {
    assert ! fragments.containsKey(fragment.getHost());
    fragments.put(fragment.getHost(), fragment);
  }
  
  /**
   * Merge a set of fragments for this slice. We assume each slice
   * runs on a distinct host.
   */
  public SliceNode merge(SliceNode other) {
    assert queryId == other.queryId;
    fragments.putAll(other.fragments);
    return this;
  }
  
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queryId", queryId)
        .add("fragments", fragments)
        .toString();
  }
  
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof SliceNode)) {
      return false;
    }
    SliceNode other = (SliceNode) obj;
    return queryId.equals(queryId) &&
           fragments.equals(other.fragments);
  }   
}
