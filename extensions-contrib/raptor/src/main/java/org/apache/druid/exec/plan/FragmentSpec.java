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

package org.apache.druid.exec.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class FragmentSpec
{
  private final int sliceId;
  private final int fragmentId;
  private final int rootId;
  private final List<OperatorSpec> operators;

  @JsonCreator
  public FragmentSpec(
      @JsonProperty("sliceId") final int sliceId,
      @JsonProperty("fragmentId") final int fragmentId,
      @JsonProperty("rootId") final int rootId,
      @JsonProperty("operators") final List<OperatorSpec> operators)
  {
    this.sliceId = sliceId;
    this.fragmentId = fragmentId;
    this.rootId = rootId;
    this.operators = operators;
  }

  @JsonProperty("sliceId")
  public int sliceId()
  {
    return sliceId;
  }

  @JsonProperty("fragmentId")
  public int fragmentId()
  {
    return fragmentId;
  }

  @JsonProperty("rootId")
  public int rootId()
  {
    return rootId;
  }

  @JsonProperty("operators")
  public List<OperatorSpec> operators()
  {
    return operators;
  }
}
