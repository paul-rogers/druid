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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

public class RootFragmentProfile extends FragmentProfile
{
  /**
   * Optional address of the client which sent the query.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String remoteAddress;
  /**
   * Query ID assigned to the query by the receiving host.
   */
  @JsonProperty
  public String queryId;
  /**
   * Columns required to process the query.
   */
  @JsonProperty
  public List<String> columns;
  
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
    RootFragmentProfile other = (RootFragmentProfile) o;
    return Objects.equal(remoteAddress, other.remoteAddress) &&
        Objects.equal(queryId, other.queryId) &&
        Objects.equal(columns, other.columns);
  }
  
  @Override
  protected ToStringHelper toStringHelper() {
    return super.toStringHelper()
        .add("remoteAddress", remoteAddress)
        .add("queryId", queryId)
        .add("columns", columns);
  }
}
