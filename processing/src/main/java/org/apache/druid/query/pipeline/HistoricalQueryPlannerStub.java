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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
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
    final QueryToolChest<T, Query<T>> toolChest;

    public FactoryHolder(
        final QueryRunnerFactory<T, Query<T>> factory,
        final QueryToolChest<T, Query<T>> toolChest)
    {
      this.factory = factory;
      this.toolChest = toolChest;
    }

    public QueryRunnerFactory<T, Query<T>> factory()
    {
      return factory;
    }

    public QueryToolChest<T, Query<T>> toolChest()
    {
      return toolChest;
    }

    public QueryMetrics<?> metrics(Query<?> query)
    {
      @SuppressWarnings("unchecked")
      final QueryPlus<?> queryWithMetrics = QueryPlus.wrap((Query<T>) query).withQueryMetrics(toolChest);
      return queryWithMetrics.getQueryMetrics();
    }
  }

  private final FactoryHolder<?> holder;
  private final QueryPlanner queryPlanner;
  private final ServiceEmitter emitter;
  private final FragmentRunner runner = new FragmentRunner();

  public <T> HistoricalQueryPlannerStub(
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final ServiceEmitter emitter)
  {
    this.holder = new FactoryHolder<T>(factory, toolChest);
    // TODO: Obtain the planner from the factory
    Preconditions.checkArgument(((Object) factory) instanceof ScanQueryRunnerFactory);
    this.queryPlanner = new ScanQueryPlanner();
    this.emitter = emitter;
  }

  public <T> QueryRunner<T> plan(final Query<?> query,
      SegmentReference segment,
      SegmentDescriptor descriptor)
  {
    planMetrics(query, segment,
      planRefCount(segment, descriptor,
          planScan(query, segment)));
    return runner.toRunner();
  }

  public Operator planMetrics(
      final Query<?> query,
      SegmentReference segment,
      Operator child)
  {
    return runner.add(
        new MetricsOperator(
          emitter,
          segment.getId().toString(),
          holder.metrics(query),
          child));
  }

  public Operator planRefCount(
      SegmentReference segment,
      SegmentDescriptor descriptor,
      Operator child)
  {
    return runner.add(new SegmentLockOperator(segment, descriptor, child));
  }

  private Operator planScan(Query<?> query,
      SegmentReference segment)
  {
    return runner.add(queryPlanner.planScan(query, segment));
  }
}
