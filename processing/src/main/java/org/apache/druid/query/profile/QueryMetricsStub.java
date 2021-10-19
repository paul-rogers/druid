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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;

import java.util.List;

public class QueryMetricsStub<QueryType extends Query<?>> implements QueryMetricsAdapter<QueryType>
{
  private final ResponseContext responseContext;

  public QueryMetricsStub(ResponseContext context)
  {
    this.responseContext = context;
  }

  @Override
  public void query(QueryType query)
  {
  }

  @Override
  public void dataSource(QueryType query)
  {
  }

  @Override
  public void queryType(QueryType query)
  {
  }

  @Override
  public void interval(QueryType query)
  {
  }

  @Override
  public void hasFilters(QueryType query)
  {
  }

  @Override
  public void duration(QueryType query)
  {
  }

  @Override
  public void queryId(QueryType query)
  {
  }

  @Override
  public void subQueryId(QueryType query)
  {
  }

  @Override
  public void sqlQueryId(QueryType query)
  {
  }

  @Override
  public void context(QueryType query)
  {
  }

  @Override
  public void server(String host)
  {
  }

  @Override
  public void remoteAddress(String remoteAddress)
  {
  }

  @Override
  public void status(String status)
  {
  }

  @Override
  public void success(boolean success)
  {
  }

  @Override
  public void segment(String segmentIdentifier)
  {
  }

  @Override
  public void preFilters(List<Filter> preFilters)
  {
  }

  @Override
  public void postFilters(List<Filter> postFilters)
  {
  }

  @Override
  public void identity(String identity)
  {
  }

  @Override
  public void vectorized(boolean vectorized)
  {
  }

  @Override
  public void parallelMergeParallelism(int parallelism)
  {
  }

  @Override
  public BitmapResultFactory<?> makeBitmapResultFactory(BitmapFactory factory)
  {
    return null;
  }

  @Override
  public QueryMetrics<QueryType> reportQueryTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportQueryBytes(long byteCount)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportQueriedSegmentCount(long segmentCount)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportWaitTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentAndCacheTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportCpuTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTimeToFirstByte(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportBackPressureTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportNodeBytes(long byteCount)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportNodeRows(long numRows)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportBitmapConstructionTime(long timeNs)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentRows(long numRows)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportPreFilteredRows(long numRows)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeParallelism(int parallelism)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputSequences(long numSequences)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputRows(long numRows)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeOutputRows(long numRows)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTaskCount(long numTasks)
  {
    return this;
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalCpuTime(long timeNs)
  {
    return this;
  }

  @Override
  public void emit(ServiceEmitter emitter, ResponseContext responseContext)
  {
  }

  @Override
  public void pushProfile(OperatorProfile profile)
  {
    responseContext.pushProfile(profile);
  }
}
