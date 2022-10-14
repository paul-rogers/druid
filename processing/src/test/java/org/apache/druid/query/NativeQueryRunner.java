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

package org.apache.druid.query;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.QueryManager;
import org.apache.druid.queryng.fragment.QueryManagerFactory;
import org.apache.druid.queryng.fragment.TestQueryManagerFactory;

import java.util.List;

/**
 * Native query runner which allows testing of either the
 * "classic" or "operator" execution paths, depending on the command-line
 * setting of the "Query NG" flag.
 *
 * @see {@link TestQueryManagerFactory} for config details
 */
public class NativeQueryRunner
{
  protected static final QueryManagerFactory QUERY_MANAGER_FACTORY = new TestQueryManagerFactory();

  public static <T> Sequence<T> run(QueryRunner<T> runner, Query<T> query)
  {
    ResponseContext responseContext = ResponseContext.createEmpty();
    final QueryManager queryManager = QUERY_MANAGER_FACTORY.create(query);
    final FragmentManager fragment = queryManager == null ? null : queryManager.createRootFragment(responseContext);
    return runner.run(
        QueryPlus.wrap(query).withFragment(fragment),
        responseContext
    );
  }

  public static <T> List<T> runToList(QueryRunner<T> runner, Query<T> query)
  {
    return run(runner, query).toList();
  }
}
