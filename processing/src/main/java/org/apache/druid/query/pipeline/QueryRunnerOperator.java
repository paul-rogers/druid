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

import java.util.Iterator;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;

public class QueryRunnerOperator<T> implements Operator
{
  private final QueryRunner<T> runner;
  private final QueryPlus<T> query;
  private Operator child;

  public QueryRunnerOperator(QueryRunner<T> runner, QueryPlus<T> query) {
    this.runner = runner;
    this.query = query;
  }

  @Override
  public Iterator<Object> open(FragmentContext context) {
    Sequence<T> seq = runner.run(query, context.responseContext());
    child = Operators.toOperator(seq);
    return child.open(context);
  }

  @Override
  public void close(boolean cascade) {
    if (child != null && cascade) {
      child.close(cascade);
    }
    child = null;
  }
}
