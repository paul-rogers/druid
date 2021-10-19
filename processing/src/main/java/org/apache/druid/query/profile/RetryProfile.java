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
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the
 * {@link org.apache.druid.query.RetryQueryRunner RetryQueryRunner}
 * operation.
 */
public class RetryProfile extends OperatorProfile
{
  public static final String TYPE = "retry";

  public static class Retry
  {
    @JsonProperty
    public boolean truncated;
    @JsonProperty
    public List<SegmentDescriptor> missingSegments;

    public Retry(ResponseContext context)
    {
      truncated = context.getTruncated();
      missingSegments = context.getMissingSegments();
    }
  }

  /**
   * Child operations. Will be one if no retries occurred.
   * If retries, will include one for each retry.
   */
  @JsonProperty
  public List<OperatorProfile> children = new ArrayList<>();

  /**
   * Results from a retry: the list of missing segments and whether
   * that list was truncated.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Retry> retries;

  public void addRetry(ResponseContext context)
  {
    if (retries == null) {
      retries = new ArrayList<>();
    }
    retries.add(new Retry(context));
  }
}
