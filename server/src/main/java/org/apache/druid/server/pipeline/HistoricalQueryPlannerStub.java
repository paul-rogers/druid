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

package org.apache.druid.server.pipeline;

import java.util.Optional;

import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.pipeline.FragmentRunner;
import org.apache.druid.query.pipeline.MetricsOperator;
import org.apache.druid.query.pipeline.Operator;
import org.apache.druid.query.pipeline.ScanQueryOperator;
import org.apache.druid.query.pipeline.SegmentLockOperator;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

public class HistoricalQueryPlannerStub
{
  /**
   * Holds the query-independent information needed for
   * query planning.
   */
  public static class PlanContext
  {
    protected final ServiceEmitter emitter;
    protected final CacheConfig cacheConfig;
    protected final Cache cache;
    protected final CachePopulator cachePopulator;
    protected final ObjectMapper objectMapper;
    protected final JoinableFactoryWrapper joinableFactoryWrapper;

    public PlanContext(
        final ServiceEmitter emitter,
        final CacheConfig cacheConfig,
        final Cache cache,
        final CachePopulator cachePopulator,
        final ObjectMapper objectMapper,
        final JoinableFactoryWrapper joinableFactoryWrapper
    )
    {
      this.emitter = emitter;
      this.cacheConfig = cacheConfig;
      this.cache = cache;
      this.cachePopulator = cachePopulator;
      this.objectMapper = objectMapper;
      this.joinableFactoryWrapper = joinableFactoryWrapper;
    }
  }

  public interface QueryTypePlanner
  {
    Operator planScan(Query<?> query, SegmentReference segment);
  }

  public static class ScanQueryPlanner implements QueryTypePlanner
  {
    @Override
    public Operator planScan(Query<?> query, SegmentReference segment) {
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }
      return new ScanQueryOperator((ScanQuery) query, segment);
    }
  }

  /**
   * Holds the query-dependent information needed for
   * query planning. This is information about the query as
   * a whole in it original, unrewritten form.
   */
  public static class QueryPlanner<T>
  {
    final PlanContext planContext;
    @SuppressWarnings("unused")
    final Query<T> query;
    @SuppressWarnings("unused")
    final QueryRunnerFactory<T, Query<T>> factory;
    final QueryToolChest<T, Query<T>> toolChest;
    @SuppressWarnings("unused")
    final DataSourceAnalysis analysis;
    final QueryTypePlanner queryTypePlanner;
    final Optional<byte[]> cacheKeyPrefix;
    final FragmentRunner runner = new FragmentRunner();

    public QueryPlanner(
        final PlanContext planContext,
        final Query<T> query,
        final QueryRunnerFactory<T, Query<T>> factory,
        final DataSourceAnalysis analysis)
    {
      this.planContext = planContext;
      this.query = query;
      this.factory = factory;
      this.toolChest = factory.getToolchest();
      this.analysis = analysis;
      // TODO: Obtain the planner from the factory
      Preconditions.checkArgument(((Object) factory) instanceof ScanQueryRunnerFactory);
      this.queryTypePlanner = new ScanQueryPlanner();
      // We compute the join cache key here itself so it doesn't need to be re-computed for every segment
      this.cacheKeyPrefix = analysis.isJoin()
          ? planContext.joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis)
          : Optional.of(StringUtils.EMPTY_BYTES);
    }

    Operator add(Operator op)
    {
      return runner.add(op);
    }
  }

  /**
   * Holds query- and fragment-dependent information needed
   * to plan the per-fragment parts of a query.
   */
  public static class SegmentPlanner<T>
  {
    final PlanContext planContext;
    final QueryPlanner<T> queryPlanner;
    final Query<T> query;
    final SegmentReference segment;
    final SegmentDescriptor descriptor;
    final String segmentIdString;
    final Interval actualDataInterval;

    public SegmentPlanner(
        final QueryPlanner<T> queryPlanner,
        final Query<T> query,
        final SegmentReference segment,
        final SegmentDescriptor descriptor
        )
    {
      this.planContext = queryPlanner.planContext;
      this.queryPlanner = queryPlanner;
      this.query = query;
      this.segment = segment;
      this.descriptor = descriptor;
      SegmentId segmentId = segment.getId();
      this.segmentIdString = segmentId.toString();
      StorageAdapter storageAdapter = segment.asStorageAdapter();
      long segmentMaxTime = storageAdapter.getMaxTime().getMillis();
      long segmentMinTime = storageAdapter.getMinTime().getMillis();
      this.actualDataInterval = Intervals.utc(segmentMinTime, segmentMaxTime + 1);
    }

    public Operator plan()
    {
      return planCache(
          planMetrics(
            planRefCount(
                planScan())));
    }

    public Operator planCache(Operator child)
    {
      if (!queryPlanner.cacheKeyPrefix.isPresent()) {
        return child;
      }
      final CacheStrategy<T, Object, Query<T>> strategy = queryPlanner.toolChest.getCacheStrategy(query);
      boolean populate = canPopulateCache(strategy);
      boolean use = canUseCache(strategy);
      if (!populate && !use) {
        return child;
      }
      Cache.NamedKey key = CacheUtil.computeSegmentCacheKey(
          segmentIdString,
          alignToActualDataInterval(),
          Bytes.concat(queryPlanner.cacheKeyPrefix.get(), strategy.computeCacheKey(query))
      );
      Operator op = child;
      if (populate) {
        op = queryPlanner.add(new ToCacheOperator(
            strategy,
            planContext.cache,
            key,
            planContext.cachePopulator,
            op));
      }
      if (use) {
        op = queryPlanner.add(new FromCacheOperator(
            strategy,
            planContext.cache,
            key,
            planContext.objectMapper,
            op));
      }
      return op;
    }

    public Operator planMetrics(Operator child)
    {
      final QueryPlus<T> queryWithMetrics = QueryPlus.wrap(query).withQueryMetrics(queryPlanner.toolChest);
      return queryPlanner.add(
          new MetricsOperator(
              planContext.emitter,
              segment.getId().toString(),
              queryWithMetrics.getQueryMetrics(),
              child));
    }

    public Operator planRefCount(Operator child)
    {
      return queryPlanner.add(new SegmentLockOperator(segment, descriptor, child));
    }

    private Operator planScan()
    {
      return queryPlanner.add(queryPlanner.queryTypePlanner.planScan(query, segment));
    }

    /**
     * @return whether the segment level cache should be used or not. False if strategy is null
     */
    @VisibleForTesting
    boolean canUseCache(CacheStrategy<T, Object, Query<T>> strategy)
    {
      return queryPlanner.cacheKeyPrefix.isPresent() &&
          CacheUtil.isUseSegmentCache(
            query,
            strategy,
            planContext.cacheConfig,
            CacheUtil.ServerType.DATA
          );
    }

    /**
     * @return whether the segment level cache should be populated or not. False if strategy is null
     */
    @VisibleForTesting
    boolean canPopulateCache(CacheStrategy<T, Object, Query<T>> strategy)
    {
      return queryPlanner.cacheKeyPrefix.isPresent() &&
          CacheUtil.isPopulateSegmentCache(
            query,
            strategy,
            planContext.cacheConfig,
            CacheUtil.ServerType.DATA
          );
    }

    private SegmentDescriptor alignToActualDataInterval()
    {
      Interval interval = descriptor.getInterval();
      return new SegmentDescriptor(
          interval.overlaps(actualDataInterval) ? interval.overlap(actualDataInterval) : interval,
          descriptor.getVersion(),
          descriptor.getPartitionNumber()
      );
    }
  }

  public static <T> QueryRunner<T> planSegmentStub(
      final ServiceEmitter emitter,
      final CacheConfig cacheConfig,
      final Cache cache,
      final CachePopulator cachePopulator,
      final ObjectMapper objectMapper,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final Query<T> query,
      final QueryRunnerFactory<T, Query<T>> factory,
      final DataSourceAnalysis analysis,
      final SegmentReference segment,
      final SegmentDescriptor descriptor
  )
  {
    final PlanContext context = new PlanContext(
        emitter,
        cacheConfig,
        cache,
        cachePopulator,
        objectMapper,
        joinableFactoryWrapper
        );
    // For now, use the same segment-specific query for both query and segment.
    final QueryPlanner<T> queryPlanner = new QueryPlanner<>(
        context,
        query,
        factory,
        analysis
        );
    return planSegmentStub(queryPlanner, query, segment, descriptor);
  }

  public static <T> QueryRunner<T> planSegmentStub(
      final QueryPlanner<T> queryPlanner,
      final Query<T> query,
      final SegmentReference segment,
      final SegmentDescriptor descriptor
  )
  {
    SegmentPlanner<T> segmentPlanner = new SegmentPlanner<>(
        queryPlanner,
        query,
        segment,
        descriptor
        );

    // Ignore the root, we'll get it from the runner.
    segmentPlanner.plan();

    // Convert the fragment runner to a query runner.
    return queryPlanner.runner.toRunner();
  }

//  public static class CachePlanner
//  {
//     private final CacheConfig cacheConfig;
//
//    public CachePlanner(
//        final CacheConfig cacheConfig,
//        final DataSourceAnalysis analysis,
//        final JoinableFactoryWrapper joinableFactoryWrapper)
//    {
//      this.cacheConfig = cacheConfig;
//    }
//   }

//  private final FactoryHolder<?> holder;
//  private final Cache cache;
//  private final CachePlanner cachePlanner;
//
//  public <T> HistoricalQueryPlannerStub(
//      final QueryRunnerFactory<T, Query<T>> factory,
//      final QueryToolChest<T, Query<T>> toolChest,
//      final ServiceEmitter emitter,
//      final Cache cache,
//      final CacheConfig cacheConfig,
//      final DataSourceAnalysis analysis,
//      final JoinableFactoryWrapper joinableFactoryWrapper)
//  {
//    this.holder = new FactoryHolder<T>(factory, toolChest);
//    this.emitter = emitter;
//    this.cache = cache;
//    this.cachePlanner = new CachePlanner(cacheConfig, analysis, joinableFactoryWrapper);
//  }

}
