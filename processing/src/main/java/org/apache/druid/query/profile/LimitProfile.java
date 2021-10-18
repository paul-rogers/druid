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

/**
 * Represents a limit operator:
 * {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator ScanQueryLimitRowIterator}.
 */
public class LimitProfile extends OperatorProfile
{
  public static final String TYPE = "limit";
  
  /**
   * The limit, which may represent a smaller value than specified
   * in the query if prior operations already produced some rows.
   */
  @JsonProperty
  public long limit;
  
  /**
   * If a prior operation produced some rows, this is the
   * starting row for this limit operator.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long offset;
  
  /**
   * Number of rows produced by this operator: less than or equal
   * to the limit.
   */
  @JsonProperty
  public long rows;
  
  /**
   * Number of batches received by this operator.
   */
  @JsonProperty
  public long batches;
  
  /**
   * The upstream operator which produces the rows/batches
   * to limit.
   */
  @JsonProperty
  public OperatorProfile child;
}
