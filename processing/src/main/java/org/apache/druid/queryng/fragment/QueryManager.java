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

package org.apache.druid.queryng.fragment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.operators.Operator;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

public class QueryManager
{
  private final String queryId;
  private final Map<FragmentManager, Map<Operator<?>, FragmentManager>> relationships = new IdentityHashMap<>();
  private FragmentManager rootFragment;

  public QueryManager(String queryId)
  {
    this.queryId = queryId;
  }

  @VisibleForTesting
  public static QueryManager createWithRoot(
      String queryId,
      final ResponseContext responseContext
  )
  {
    QueryManager queryManager = new QueryManager(queryId);
    queryManager.createRootFragment(responseContext);
    return queryManager;
  }

  public FragmentManager createRootFragment(final ResponseContext responseContext)
  {
    FragmentManager fragment = new FragmentManager(
        this,
        queryId,
        responseContext
    );
    registerRootFragment(fragment);
    return fragment;
  }

  public String queryId()
  {
    return queryId;
  }

  public void registerRootFragment(FragmentManager root)
  {
    Preconditions.checkState(rootFragment == null);
    rootFragment = root;
  }

  public synchronized void registerFragment(
      FragmentManager parentFagment,
      Operator<?> parentLeaf,
      FragmentManager child
  )
  {
    Map<Operator<?>, FragmentManager> children = relationships.get(parentFagment);
    if (children == null) {
      children = new HashMap<>();
      relationships.put(parentFagment, children);
    }
    children.put(parentLeaf, child);
  }

  public FragmentManager rootFragment()
  {
    return rootFragment;
  }
}
