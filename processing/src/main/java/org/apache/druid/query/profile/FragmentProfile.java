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

import javax.annotation.Nullable;

import org.apache.druid.query.Query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
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
 * into an operator for purposes of explaining the query.
 * <p>
 * Instances are compared only in tests. Instances are not used as
 * hash keys.
 */
public class FragmentProfile
{
  /**
   * Name of the host + port number that processed this fragment.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public final String host;
  /**
   * Druid service running on the host.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public final String service;
  /**
   * Optional address of the client which sent the query.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public final String remoteAddress;
  /**
   * Native query as received by the host.
   */
  @JsonProperty
  public final Query<?> query;
  /**
   * Columns required to process the query.
   */
  @JsonProperty
  public final List<String> columns;
  /**
   * Unix timestamp of the query start time.
   */
  @JsonProperty
  public final long startTime;
  /**
   * Query run time, in ns, on this host. Since the response includes
   * this object, the run time excludes the last bit of time required to
   * write the trailer that contains this profile.
   */
  @JsonProperty
  public final long timeNs;
  /**
   * CPU time to run the query, excluding top-level network deserialization
   * and serialization.
   */
  @JsonProperty
  public final long cpuNs;
  /**
   * Number of rows returned by the query. The concept of "rows" is somewhat
   * ill-defined in Druid: this is the number that most closely responds to what
   * a SQL user might think of as a row.
   */
  @JsonProperty
  public final long rows;
  /**
   * The top-most "operator" (function, transform) for this fragment.
   */
  @JsonProperty
  public final OperatorProfile rootOperator;
  
  @JsonCreator
  public FragmentProfile(
      @JsonProperty("host") @Nullable String host,
      @JsonProperty("service") @Nullable String service,
      @JsonProperty("remoteAddress") @Nullable String remoteAddress,
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("startTime") long startTime,
      @JsonProperty("timeNs") long timeNs,
      @JsonProperty("cpuNs") long cpuNs,
      @JsonProperty("rows") long rows,
      @JsonProperty("rootOperator") OperatorProfile rootOperator
  )
  {
    assert host != null;
    this.host = host;
    this.service = service;
    this.query = query;
    this.columns = columns;
    this.remoteAddress = remoteAddress;
    this.startTime = startTime;
    this.timeNs = timeNs;
    this.cpuNs = cpuNs;
    this.rows = rows;
    this.rootOperator = rootOperator;
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
           service.equals(other.service) &&
           remoteAddress.equals(other.remoteAddress) &&
           // Used only for testing: checking the type is sufficient
           isSameQueryType(query, other.query) &&
           columns.equals(other.columns) && 
           startTime == other.startTime &&
           timeNs == other.timeNs &&
           cpuNs == other.cpuNs &&
           rows == other.rows &&
           rootOperator.getClass() == other.rootOperator.getClass();
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
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("host", host)
        .add("service", service)
        .add("remoteAddress", remoteAddress)
        .add("query", query)
        .add("columns", columns)
        .add("startTime", startTime)
        .add("timeNs", timeNs)
        .add("cpuNs", cpuNs)
        .add("rows", rows)
        .add("rootOperator", rootOperator)
        .toString();
  }
}
