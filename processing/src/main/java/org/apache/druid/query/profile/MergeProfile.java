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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an n-way merge. Because of the functional style of programming,
 * it is not possible to obtain things like the detailed merge time or the
 * row count. These can be inferred, however, as the sum of any incoming row
 * counts, and as the time for this operator minus the sum of the times of
 * incoming operators.
 */
public class MergeProfile extends OperatorProfile
{
  @JsonProperty
  public List<OperatorProfile> children;
  
  @JsonCreator
  public MergeProfile() {
  }
  
  public MergeProfile(List<OperatorProfile> children) {
    this.children = children;
  }
}
