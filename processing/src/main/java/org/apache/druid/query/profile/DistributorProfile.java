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

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.joda.time.Interval;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holds statistics from the
 * {@link org.apache.druid.client.CachingClusteredClient CachingClusteredClient}
 * class and its associated mechanisms.
 * <p>
 * A parent query is distributed to one or more data nodes, and the
 * results merged. If a single node is needed, the call to the data node
 * is done in a single thread. If multiple nodes, then the requests run
 * in parallel.
 */
public class DistributorProfile extends OperatorProfile
{
  public static final String TYPE = "scatter";
  
  /**
   * Empty will be true if the datasource is unknown or is no data
   * for the given time range.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean empty;
  
  /**
   * Segment results retrieved from cache.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int fromCache;
  
  /**
   * Number of segment results added to the cache.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int toCache;
  
  /**
   * Count of segments mapped to servers. (Each server may be mapped
   * from multiple segments.) 
   */
  @JsonProperty
  public int segments;
  
  /**
   * Count of servers to that contain segments for the query. Will
   * be equal to or less than the segments value.
   */
  @JsonProperty
  public int servers;
  
  /**
   * Number of unavailable servers. The query ignored segments
   * from these servers and may be incomplete. 
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int missingServers;
  
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int uncoveredIntervals;
  
  /**
   * Optional priority assigned to the query.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer priority;
  
  /**
   * Optional lane assigned to the query.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String lane;
  
  /**
   * Operators that implement the per-server queries.
   */
  @JsonProperty
  public List<OperatorProfile> children;
  
  public void setPriority(Query<?> query) {
    priority = (Integer) query.getContext().get(QueryContexts.PRIORITY_KEY);
    lane = (String) query.getContext().get(QueryContexts.LANE_KEY);
  }
}
