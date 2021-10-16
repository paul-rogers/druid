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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Represents the execution of a ScanQuery. This operator can not track
 * its time, the timeNs field is always 0. Time is spent in the child
 * operators.
 * <p>
 * The scan query simply runs a set of child queries, one per fragment,
 * and merges the results. The strategy of the merge depends on the
 * kind of ordering requested.
 */
@JsonPropertyOrder({"strategy", "limit", "child", })
public class ScanQueryProfile extends OperatorProfile
{
  public static final String TYPE = "scan-query";
  
  /**
   * No ordering, just concatenate the results.
   */
  public static final String CONCAT_STRATEGY = "concat";
  /**
   * The result size is limited, a priority queue is used.
   */
  public static final String PQUEUE_STRATEGY = "priority-queue";
  /**
   * Results are already ordered, and are just merged.
   */
  public static final String MERGE_STRATEGY = "merge";
  
  @JsonProperty
  public String strategy;
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long limit;
  @JsonProperty
  public OperatorProfile child;
}
