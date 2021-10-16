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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Profile of an "operator" in Druid. A typical query is composed of a series
 * of (relational or other) operators strung together to process a data flow.
 * Druid does not really use operators, having more of a functional architecture.
 * However, the functions essentially do the work that a traditional operator
 * would do, so we retain the traditional terminology.
 * <p>
 * An operator is any function in Druid that obtains, transforms, combines
 * or otherwise operates on data. Trivial QueryRunners are omitted as they
 * provide no performance-related metrics. Thus, the tree of operators is a
 * simplified, truncated view of the Druid functional call stack over the life
 * of the query.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = OperatorProfile.OpaqueOperator.TYPE, value = OperatorProfile.OpaqueOperator.class),
    @JsonSubTypes.Type(name = ReceiverProfile.TYPE, value = ReceiverProfile.class),
    @JsonSubTypes.Type(name = SegmentMetadataScanProfile.TYPE, value = SegmentMetadataScanProfile.class),
    @JsonSubTypes.Type(name = MergeProfile.TYPE, value = MergeProfile.class),
    @JsonSubTypes.Type(name = ScanQueryProfile.TYPE, value = ScanQueryProfile.class),
    @JsonSubTypes.Type(name = SortProfile.TYPE, value = SortProfile.class),
    @JsonSubTypes.Type(name = ConcatProfile.TYPE, value = ConcatProfile.class),
    @JsonSubTypes.Type(name = SegmentScanProfile.TYPE, value = SegmentScanProfile.class),
    @JsonSubTypes.Type(name = IndexScanProfile.TYPE, value = IndexScanProfile.class),
    @JsonSubTypes.Type(name = RetryProfile.TYPE, value = RetryProfile.class),
})
public abstract class OperatorProfile
{
  /**
   * The total wall clock time, in ns, taken by this operator.
   * Includes the time of all child operators. The time for just
   * this operator <i>should</i> be the total time minus the time
   * for all children.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long timeNs;
  
  /**
   * A temporary placeholder for places that don't yet report
   * an operator profile.
   */
  public static class OpaqueOperator extends OperatorProfile
  {
    public static final String TYPE = "unknown";
  }
}
