package org.apache.druid.query.pipeline;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.SegmentReference;

import com.google.common.base.Preconditions;

public class HistoricalQueryPlannerStub
{
  public interface QueryPlanner
  {
    Operator planScan(Query<?> query, SegmentReference segment);
  }

  public static class ScanQueryPlanner implements QueryPlanner
  {
    @Override
    public Operator planScan(Query<?> query, SegmentReference segment) {
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }
      return new ScanQueryOperator((ScanQuery) query, segment);
    }
  }

  public static class FactoryHolder<T>
  {
    final QueryRunnerFactory<T, Query<T>> factory;

    public FactoryHolder(final QueryRunnerFactory<T, Query<T>> factory)
    {
      this.factory = factory;
    }
  }

  private final FactoryHolder<?> holder;
  private final QueryPlanner queryPlanner;

  public <T> HistoricalQueryPlannerStub(final QueryRunnerFactory<T, Query<T>> factory)
  {
    this.holder = new FactoryHolder<T>(factory);
    // TODO: Obtain the planner from the factory
    Preconditions.checkArgument(((Object) factory) instanceof ScanQueryRunnerFactory);
    this.queryPlanner = new ScanQueryPlanner();
  }

  public Operator planRefCount(Query<?> query,
      SegmentReference segment,
      SegmentDescriptor descriptor)
  {
    Operator scan = queryPlanner.planScan(query, segment);
    return new SegmentLockOperator(segment, descriptor, scan);
  }
}
