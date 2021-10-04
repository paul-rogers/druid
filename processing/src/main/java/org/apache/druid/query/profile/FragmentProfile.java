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

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * JSON-serializable description of a query request. A query request
 * is a "fragment" of a larger query: it may be the only fragment, or it
 * may be one of many parallel queries scattered across data nodes.
 * <p>
 * A fragment consists of a tree of operators, where "operator" means some
 * operation within a query that produces interesting statistics. Druid is not
 * really based on a DAG of operators, but it does have certain repeated patterns,
 * such as scans or merges, which can, if we look at them sideways, be abstracted
 * into an operator for purposes of explaing the query.
 * <p>
 * Instances are compared only in tests. Instances are not used as
 * hash keys.
 */
public class FragmentProfile
{
  private final String host;
  private final String remoteAddress;
  private final long startTime;
  private final long durationMs;
  private final long rows;
  private final OperatorProfile rootOperator;
  
  @JsonCreator
  public FragmentProfile(
      @JsonProperty("host") @Nullable String host,
      @JsonProperty("remoteAddress") @Nullable String remoteAddress,
      @JsonProperty("startTime") long startTime,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("rows") long rows,
      @JsonProperty("rootOperator") OperatorProfile rootOperator
  )
  {
    assert host != null;
    this.host = host;
    this.remoteAddress = remoteAddress;
    this.startTime = startTime;
    this.durationMs = durationMs;
    this.rows = rows;
    this.rootOperator = rootOperator;
  }
  
  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public String getRemoteAddress()
  {
    return remoteAddress;
  }

  @JsonProperty
  public long getStartTime()
  {
    return startTime;
  }

  @JsonProperty
  public long getDurationMs()
  {
    return durationMs;
  }

  @JsonProperty
  public long getRows()
  {
    return rows;
  }
  
  @JsonProperty
  public OperatorProfile getRootOperator()
  {
    return rootOperator;
  }
  
  /**
   * Primarily for testing. Ensures that the scalar fields are equal,
   * does not do a deep compare of operators.
   */
  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof FragmentProfile)) {
      return false;
    }
    FragmentProfile other = (FragmentProfile) o;
    return host.equals(other.host) &&
           remoteAddress.equals(other.remoteAddress) &&
           startTime == other.startTime &&
           durationMs == other.durationMs &&
           rows == other.rows &&
           rootOperator.getClass() == other.rootOperator.getClass();
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("host", host)
        .add("remoteAddress", remoteAddress)
        .add("startTime", startTime)
        .add("durationMs", durationMs)
        .add("rows", rows)
        .add("rootOperator", rootOperator)
        .toString();
  }
}
