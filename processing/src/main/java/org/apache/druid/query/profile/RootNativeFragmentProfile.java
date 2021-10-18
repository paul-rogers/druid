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

import org.apache.druid.query.Query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Root fragment (top-level query) for a native query, whether received
 * by the Broker or a data node.
 */
@JsonPropertyOrder({"version", "type", "host", "service", "queryId", "remoteAddress",
  "columns", "startTime", "timeNs", "cpuNs", "rows", "query", "rootOperator"})
public class RootNativeFragmentProfile extends RootFragmentProfile
{
  public static final String QUERY_TYPE = "native";
  
  /**
   * Original, unrewritten native query as received by the host,
   * typically without query ID or context.
   */
  @JsonProperty
  public Query<?> query;
  
  public RootNativeFragmentProfile() {
    super(QUERY_TYPE);
  }
  
  public static boolean isSameQueryType(Query<?> query1, Query<?> query2) {
    if (query1 == null && query2 == null) {
      return true;
    }
    if (query1 == null || query2 == null) {
      return false;
    }
    return query1.getClass() == query2.getClass() ;
  }
  
  /**
   * Primarily for testing. Ensures that the scalar fields are equal,
   * does not do a deep compare of operators.
   */
  @Override
  public boolean equals(Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    RootNativeFragmentProfile other = (RootNativeFragmentProfile) o;
    // Used only for testing: checking the type is sufficient
    return isSameQueryType(query, other.query);
  }
  
  @Override
  public String toString() {
    return toStringHelper()
        .add("query", query)
        .toString();
  }
}
