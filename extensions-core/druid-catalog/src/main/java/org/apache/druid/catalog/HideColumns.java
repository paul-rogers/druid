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

package org.apache.druid.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HideColumns
{
  @JsonProperty
  public final List<String> hide;
  @JsonProperty
  public final List<String> unhide;

  @JsonCreator
  public HideColumns(
      @Nullable final List<String> hide,
      @Nullable final List<String> unhide
  )
  {
    this.hide = hide;
    this.unhide = unhide;
  }

  public boolean isEmpty()
  {
    return (hide == null || hide.isEmpty())
        && (unhide == null || unhide.isEmpty());
  }

  public List<String> perform(List<String> hiddenColumns)
  {
    if (hiddenColumns == null) {
      hiddenColumns = Collections.emptyList();
    }
    Set<String> existing = new HashSet<>(hiddenColumns);
    if (unhide != null) {
      for (String col : unhide) {
        existing.remove(col);
      }
    }
    List<String> revised = new ArrayList<>();
    for (String col : hiddenColumns) {
      if (existing.contains(col)) {
        revised.add(col);
      }
    }
    if (hide != null) {
      for (String col : hide) {
        if (!existing.contains(col)) {
          revised.add(col);
        }
      }
    }
    return revised.isEmpty() ? null : revised;
  }
}
