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
import org.apache.druid.query.profile.OperatorProfile.BranchingOperatorProfile;
import org.apache.druid.timeline.SegmentId;

@JsonPropertyOrder({"segment", "interval", "columnCount", "isWildcard",
    "batchSize", "limited", "cursors", "rows", "error"})
public class SegmentScanProfile extends BranchingOperatorProfile
{
  public static final String TYPE = "segment-scan";

  /**
   * The ID of the segment which was scanned.
   */
  @JsonProperty
  public final SegmentId segment;

  /**
   * The time interval within the segment to be scanned.
   */
  @JsonProperty
  public String interval;

  /**
   * Number of columns which this scan returns.
   */
  @JsonProperty
  public int columnCount;

  /**
   * True if the query provided no columns (which is the equivalent of the
   * SQL <code>SELECT *</code> wildcard query.)
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isWildcard;

  /**
   * The size of the batches used to return rows from this scan.
   */
  @JsonProperty
  public int batchSize;

  /**
   * True if the query was limited. For example, if other parts of the query already
   * returned enough rows to satisfy the LIMIT clause, and this scan returns no rows
   * as a result.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean limited;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String error;

  public SegmentScanProfile(SegmentId segment)
  {
    this.segment = segment;
  }
}
