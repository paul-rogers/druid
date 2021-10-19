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

public class QueryMetricsShim<QueryType extends Query<?>> implements QueryMetricsAdapter<QueryType>
{
  private final QueryMetrics<QueryType> base;
  private final ResponseContext responseContext;

  public QueryMetricsShim(QueryMetrics<QueryType> base, ResponseContext context)
  {
    this.base = base;
    this.responseContext = context;
  }

  // Note: due to the awkward structure of the QueryMetrics interface,
  // None of these initial "setter" methods can every be called on this
  // shim. We implement the methods anyway to avoid confusion.
  @Override
  public void query(QueryType query)
  {
    base.query(query);
  }

  @Override
  public void dataSource(QueryType query)
  {
    base.dataSource(query);
  }

  @Override
  public void queryType(QueryType query)
  {
    base.queryType(query);
  }

  @Override
  public void interval(QueryType query)
  {
    base.interval(query);
  }

  @Override
  public void hasFilters(QueryType query)
  {
    base.hasFilters(query);
  }

  @Override
  public void duration(QueryType query)
  {
  }

  @Override
  public void queryId(QueryType query)
  {
    base.queryId(query);
  }

  @Override
  public void subQueryId(QueryType query)
  {
    base.subQueryId(query);
  }

  @Override
  public void sqlQueryId(QueryType query)
  {
    base.sqlQueryId(query);
  }

  @Override
  public void context(QueryType query)
  {
    base.context(query);
  }

  @Override
  public void server(String host)
  {
    base.server(host);
  }

  @Override
  public void remoteAddress(String remoteAddress)
  {
    base.remoteAddress(remoteAddress);
  }

  @Override
  public void status(String status)
  {
    base.segment(status);
  }

  @Override
  public void success(boolean success)
  {
    base.success(success);
  }

  @Override
  public void segment(String segmentIdentifier)
  {
    base.segment(segmentIdentifier);
  }

  @Override
  public void preFilters(List<Filter> preFilters)
  {
    base.preFilters(preFilters);
  }

  @Override
  public void postFilters(List<Filter> postFilters)
  {
    base.postFilters(postFilters);
  }

  @Override
  public void identity(String identity)
  {
    base.identity(identity);
  }

  @Override
  public void vectorized(boolean vectorized)
  {
    base.vectorized(vectorized);
  }

  @Override
  public void parallelMergeParallelism(int parallelism)
  {
    base.parallelMergeParallelism(parallelism);
  }

  @Override
  public BitmapResultFactory<?> makeBitmapResultFactory(BitmapFactory factory)
  {
    return base.makeBitmapResultFactory(factory);
  }

  // The "report" method should be helpful, but they seem to occur
  // at a time other than what we need for the profile. Just pass
  // the values through.

  @Override
  public QueryMetrics<QueryType> reportQueryTime(long timeNs)
  {
    return base.reportQueryTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportQueryBytes(long byteCount)
  {
    return base.reportQueryBytes(byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportQueriedSegmentCount(long segmentCount)
  {
    return base.reportQueriedSegmentCount(segmentCount);
  }

  @Override
  public QueryMetrics<QueryType> reportWaitTime(long timeNs)
  {
    return base.reportWaitTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentTime(long timeNs)
  {
    return base.reportSegmentTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentAndCacheTime(long timeNs)
  {
    return base.reportSegmentAndCacheTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportCpuTime(long timeNs)
  {
    return base.reportCpuTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTimeToFirstByte(long timeNs)
  {
    return base.reportNodeTimeToFirstByte(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportBackPressureTime(long timeNs)
  {
    return base.reportBackPressureTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportNodeTime(long timeNs)
  {
    return base.reportNodeTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportNodeBytes(long byteCount)
  {
    return base.reportNodeBytes(byteCount);
  }

  @Override
  public QueryMetrics<QueryType> reportNodeRows(long numRows)
  {
    return base.reportNodeRows(numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportBitmapConstructionTime(long timeNs)
  {
    return base.reportBitmapConstructionTime(timeNs);
  }

  @Override
  public QueryMetrics<QueryType> reportSegmentRows(long numRows)
  {
    return base.reportSegmentRows(numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportPreFilteredRows(long numRows)
  {
    return base.reportPreFilteredRows(numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeParallelism(int parallelism)
  {
    return base.reportParallelMergeParallelism(parallelism);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputSequences(long numSequences)
  {
    return base.reportParallelMergeInputSequences(numSequences);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputRows(long numRows)
  {
    return base.reportParallelMergeInputRows(numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeOutputRows(long numRows)
  {
    return base.reportParallelMergeOutputRows(numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTaskCount(long numTasks)
  {
    return base.reportParallelMergeTaskCount(numTasks);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalCpuTime(long timeNs)
  {
    return base.reportParallelMergeTotalCpuTime(timeNs);
  }

  @Override
  public void emit(ServiceEmitter emitter, ResponseContext responseContext)
  {
    base.emit(emitter, responseContext);
  }

  @Override
  public void pushProfile(OperatorProfile profile)
  {
    responseContext.pushProfile(profile);
  }
}
