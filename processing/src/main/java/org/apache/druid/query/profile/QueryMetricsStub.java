package org.apache.druid.query.profile;

import java.util.List;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;

public class QueryMetricsStub<QueryType extends Query<?>> implements QueryMetricsAdapter<QueryType>
{
  private final ResponseContext responseContext;

  public QueryMetricsStub(ResponseContext context) {
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
  public void pushProfile(OperatorProfile profile) {
    responseContext.pushProfile(profile);
  }
}
