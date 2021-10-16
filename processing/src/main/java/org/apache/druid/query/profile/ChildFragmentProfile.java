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

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.druid.query.Query;
import org.joda.time.Interval;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

/**
 * Fragment that represents a sub-query: a query submitted by another
 * Druid node.
 */
@JsonPropertyOrder({"host", "service",
  "columns", "rows", "startTime", "timeNs", "cpuNs", "query", "rootOperator"})
public class ChildFragmentProfile extends FragmentProfile
{
  /**
   * "Stub" of the full query. Contains only those fields rewritten
   * by the broker. Avoids the need to repeat the full query for every
   * child fragment.
   */
  public static class StubQuery
  {
    @JsonProperty
    List<Interval> intervals;
    @JsonProperty
    Map<String, Object> context;
    
    public StubQuery(Query<?> query) {
      this.intervals = query.getIntervals();
      this.context = query.getContext();
    }
    
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("intervals", intervals)
          .add("context", context)
          .toString();
    }
  }
  
  @JsonProperty
  public final StubQuery query;
  
  public ChildFragmentProfile(Query<?> query)
  {
    this.query = new StubQuery(query);
  }
  
  /**
   * Primarily for testing. Ensures that the scalar fields are equal,
   * does not do a deep compare of operators.
   */
  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof ChildFragmentProfile)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    //FragmentProfile other = (FragmentProfile) o;
    return true;
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("query", query)
        .toString();
  }
}
