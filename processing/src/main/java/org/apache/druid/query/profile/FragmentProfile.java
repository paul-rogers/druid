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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

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
 * This base class is common to the "root" fragment submitted by the client
 * and an internal fragment submitted by another Druid node.
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
  public String host;

  /**
   * Druid service running on the host.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String service;

  /**
   * Unix timestamp of the query start time.
   */
  @JsonProperty
  public long startTime;

  /**
   * Query run time, in ns, on this host. Since the response includes
   * this object, the run time excludes the last bit of time required to
   * write the trailer that contains this profile.
   */
  @JsonProperty
  public long timeNs;

  /**
   * CPU time to run the query, excluding top-level network deserialization
   * and serialization.
   */
  @JsonProperty
  public long cpuNs;

  /**
   * Number of rows returned by the query. The concept of "rows" is somewhat
   * ill-defined in Druid: this is the number that most closely responds to what
   * a SQL user might think of as a row.
   */
  @JsonProperty
  public long rows;

  /**
   * The top-most "operator" (function, transform) for this fragment.
   */
  @JsonProperty
  public OperatorProfile rootOperator;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String error;

  /**
   * Primarily for testing. Ensures that the scalar fields are equal,
   * does not do a deep compare of operators.
   */
  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FragmentProfile other = (FragmentProfile) o;
    return host.equals(other.host) &&
           service.equals(other.service) &&
           startTime == other.startTime &&
           timeNs == other.timeNs &&
           cpuNs == other.cpuNs &&
           rows == other.rows &&
           rootOperator.getClass() == other.rootOperator.getClass();
  }

  protected ToStringHelper toStringHelper()
  {
    return Objects.toStringHelper(this)
        .add("host", host)
        .add("service", service)
        .add("startTime", startTime)
        .add("timeNs", timeNs)
        .add("cpuNs", cpuNs)
        .add("rows", rows)
        .add("rootOperator", rootOperator);
  }
}
