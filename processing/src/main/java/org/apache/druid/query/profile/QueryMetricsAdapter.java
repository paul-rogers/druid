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

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;

public interface QueryMetricsAdapter<QueryType extends Query<?>> extends QueryMetrics<QueryType>
{
  ProfileStack getProfileStack();

  static <QueryType extends Query<?>> QueryMetricsAdapter<QueryType> wrap(QueryMetrics<QueryType> in, ProfileStack profileStack)
  {
    if (in == null) {
      return new QueryMetricsStub<QueryType>(profileStack);
    } else {
      return new QueryMetricsShim<QueryType>(in, profileStack);
    }
  }

  static void setProfile(QueryMetrics<?> metrics, OperatorProfile profile)
  {
    if (metrics == null) {
      return;
    }
    if (!(metrics instanceof QueryMetricsAdapter)) {
      return;
    }
    ProfileStack profileStack = ((QueryMetricsAdapter<?>) metrics).getProfileStack();
    profileStack.leaf(profile);
  }
}
