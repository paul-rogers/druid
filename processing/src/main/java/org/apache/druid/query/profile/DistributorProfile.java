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
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence.MergeCombineMetrics;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.profile.OperatorProfile.BranchingOperatorProfile;

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
@JsonPropertyOrder({"empty", "priority", "lane",
    "segments", "servers",
    "fromCache", "toCache",
    "timeNs", "rows", "batches",
    "missingServers", "uncoveredIntervals",
    "children", "parallelMerge"})
public class DistributorProfile extends BranchingOperatorProfile
{
  public static final String TYPE = "scatter";

  public class ParallelMergeProfile extends OperatorProfile
  {
    @JsonProperty
    public final int parallelism;
    @JsonProperty
    public final long inputSequences;
    @JsonProperty
    public final long inputRows;
    @JsonProperty
    public final long outputRows;
    @JsonProperty
    public final long tasks;
    @JsonProperty
    public final long cpuTime;

    public ParallelMergeProfile(MergeCombineMetrics metrics)
    {
      this.parallelism = metrics.getParallelism();
      this.inputSequences = metrics.getInputSequences();
      this.inputRows = metrics.getInputRows();
      this.outputRows = metrics.getOutputRows();
      this.tasks = metrics.getTaskCount();
      this.cpuTime = metrics.getTotalCpuTime();
    }
  }

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
   * Details of the parallel merge operation, if any.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ParallelMergeProfile parallelMerge;

  public void setPriority(Query<?> query)
  {
    priority = (Integer) query.getContext().get(QueryContexts.PRIORITY_KEY);
    lane = (String) query.getContext().get(QueryContexts.LANE_KEY);
  }

  public void parallelMerge(MergeCombineMetrics metrics)
  {
    parallelMerge = new ParallelMergeProfile(metrics);
  }
}
