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

package org.apache.druid.sql.profile;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRawValue;
import org.apache.curator.shaded.com.google.common.base.Objects;
import org.apache.druid.query.profile.RootFragmentProfile;
import org.apache.druid.sql.http.SqlQuery;

/**
 * Represents the profile information for a SQL query. Here,
 * the <code>query</code> field is the SQL query, and
 * <code>queryID</code> is the SQL query ID.
 * <p>
 * When stored, native and SQL queries are intermixed,
 * both keyed by query ID. SQL query profiles contain the
 * native queries as fragments: because the native queries
 * are fragments, they do not get their own profiles.
 */
@JsonPropertyOrder({"version", "type", "host", "service", "queryId",
    "remoteAddress", "columns", "startTime", "timeNs", "cpuNs", "rows",
    "query", "plan", "rootOperator"})
public class SqlFragmentProfile extends RootFragmentProfile
{
  public static final String QUERY_TYPE = "sql";

  /**
   * Original SQL query received by the Broker.
   */
  @JsonProperty
  public SqlQuery query;

  /**
   * Query plan from Calcite: same as EXPLAIN PLAN FOR with JSON format.
   */
  @JsonProperty
  @JsonRawValue
  public String plan;

  public SqlFragmentProfile()
  {
    super(QUERY_TYPE);
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
    SqlFragmentProfile other = (SqlFragmentProfile) o;
    return Objects.equal(query, other.query) &&
           Objects.equal(plan, other.plan);
  }

  @Override
  public String toString()
  {
    return toStringHelper()
        .add("query", query)
        .add("pan", plan)
        .toString();
  }
}
