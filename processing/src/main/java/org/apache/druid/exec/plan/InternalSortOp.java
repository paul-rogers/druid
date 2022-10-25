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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.frame.key.SortColumn;

import java.util.List;

public class InternalSortOp extends AbstractUnarySpec
{
  public enum SortType
  {
    ROW
  }

  private final SortType sortType;
  private final List<SortColumn> keys;

  public InternalSortOp(
      @JsonProperty("id") final int id,
      @JsonProperty("child") final int child,
      @JsonProperty("sortType") final SortType sortType,
      @JsonProperty("keys") final List<SortColumn> keys
  )
  {
    super(id, child);
    this.sortType = sortType;
    this.keys = keys;
  }

  @JsonProperty("sortType")
  public SortType sortType()
  {
    return sortType;
  }

  @JsonProperty("keys")
  public List<SortColumn> keys()
  {
    return keys;
  }
}
