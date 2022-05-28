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

package org.apache.druid.query.pipeline;

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;

/**
 * Temporary hack to enable, disable operators.
 */
public class OperatorConfig
{
  public static boolean enabled()
  {
    return false;
  }

  public static boolean enabledFor(Query<?> query)
  {
    if (!(query instanceof ScanQuery)) {
      return false;
    }
    return enabled();
  }

  public static boolean enabledFor(final QueryPlus<?> queryPlus)
  {
    return enabledFor(queryPlus.getQuery());
  }

  public interface FragmentHandle
  {
    void close(boolean success);
  }

  public static void setupContext(Query<?> query, ResponseContext responseContext) {
    if (!enabledFor(query)) {
      return;
    }
    FragmentContext fragmentContext = new FragmentContextImpl(
        query.getId(),
        0,
        responseContext
        );
    responseContext.setFragmentContext(fragmentContext);
  }

  public static void shutdown(ResponseContext responseContext, boolean success)
  {
    if (!responseContext.useOperators()) {
      return;
    }
    FragmentContext fragmentContext = responseContext.getFragmentContext();
    fragmentContext.close(success);
  }
}
