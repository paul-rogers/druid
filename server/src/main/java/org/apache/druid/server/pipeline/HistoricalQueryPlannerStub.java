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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;

import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.pipeline.BySegmentOperator;
import org.apache.druid.query.pipeline.ConcatOperator;
import org.apache.druid.query.pipeline.FragmentRunner;
import org.apache.druid.query.pipeline.MetricsOperator;
import org.apache.druid.query.pipeline.MissingSegmentsOperator;
import org.apache.druid.query.pipeline.Operator;
import org.apache.druid.query.pipeline.ScanQueryOperator;
import org.apache.druid.query.pipeline.ScanResultLimitOperator;
import org.apache.druid.query.pipeline.ScanResultOffsetOperator;
import org.apache.druid.query.pipeline.SegmentLockOperator;
import org.apache.druid.query.pipeline.ThreadLabelOperator;
import org.apache.druid.query.pipeline.TransformOperator;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.profile.Timer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

public class HistoricalQueryPlannerStub
{
  /**
   * Holds the query-independent information needed for
   * query planning.
   */
  public static class PlanContext
  {
    final ServerConfig serverConfig;
    final ServiceEmitter emitter;
    final CacheConfig cacheConfig;
    final Cache cache;
    final CachePopulator cachePopulator;
    final ObjectMapper objectMapper;
    final SegmentManager segmentManager;
    final JoinableFactoryWrapper joinableFactoryWrapper;

    public PlanContext(
        final ServerConfig serverConfig,
        final ServiceEmitter emitter,
        final CacheConfig cacheConfig,
        final Cache cache,
        final CachePopulator cachePopulator,
        final ObjectMapper objectMapper,
        final SegmentManager segmentManager,
        final JoinableFactoryWrapper joinableFactoryWrapper
    )
    {
      this.serverConfig = serverConfig;
      this.emitter = emitter;
      this.cacheConfig = cacheConfig;
      this.cache = cache;
      this.cachePopulator = cachePopulator;
      this.objectMapper = objectMapper;
      this.segmentManager = segmentManager;
      this.joinableFactoryWrapper = joinableFactoryWrapper;
    }

    public <T> Query<T> withTimeoutAndMaxScatterGatherBytes(Query<T> query)
    {
      return QueryContexts.verifyMaxQueryTimeout(
          QueryContexts.withMaxScatterGatherBytes(
              QueryContexts.withDefaultTimeout(
                  query,
                  Math.min(serverConfig.getDefaultQueryTimeout(), serverConfig.getMaxQueryTimeout())
              ),
              serverConfig.getMaxScatterGatherBytes()
          ),
          serverConfig.getMaxQueryTimeout()
      );
    }
  }

  /**
   * Query-type-specific query planning. Stateful: one is created for each query
   * to plan.
   */
  public interface QueryTypePlanner<T>
  {
    Operator planScan(Query<?> query, SegmentReference segment);
    Query<T> prepareMerge(Query<?> query);
    Operator mergeResults(final List<Operator> children);
  }

  /**
   * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest}
   */
  public static class ScanQueryPlanner implements QueryTypePlanner<ScanResultValue>
  {
    private final ScanQueryQueryToolChest toolChest;
    private final FragmentRunner runner;
    private long limit;
    private long offset;
    private ScanQuery queryToRun;

    public ScanQueryPlanner(QueryToolChest<?, ?> toolChest, FragmentRunner runner)
    {
      this.toolChest = (ScanQueryQueryToolChest) toolChest;
      this.runner = runner;
    }

    @Override
    public Operator planScan(Query<?> query, SegmentReference segment) {
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }
      return new ScanQueryOperator((ScanQuery) query, segment);
    }

    /**
     * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest#mergeResults}
     */
    @Override
    public Query<ScanResultValue> prepareMerge(Query<?> query) {
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }

      // Ensure "legacy" is a non-null value, such that all other nodes this query is forwarded to will treat it
      // the same way, even if they have different default legacy values.
      //
      // Also, remove "offset" and add it to the "limit" (we won't push the offset down, just apply it here, at the
      // merge at the top of the stack).
      final ScanQuery originalQuery = (ScanQuery) query;
      offset = originalQuery.getScanRowsOffset();

      if (!originalQuery.isLimited()) {
        // Unlimited stays unlimited.
        limit = Long.MAX_VALUE;
      } else if (originalQuery.getScanRowsLimit() > Long.MAX_VALUE - originalQuery.getScanRowsOffset()) {
        throw new ISE(
            "Cannot apply limit[%d] with offset[%d] due to overflow",
            originalQuery.getScanRowsLimit(),
            originalQuery.getScanRowsOffset()
        );
      } else {
        limit = originalQuery.getScanRowsLimit() + originalQuery.getScanRowsOffset();
      }
      queryToRun = originalQuery.withNonNullLegacy(toolChest.getConfig())
          .withOffset(0)
          .withLimit(limit);
      return queryToRun;
    }

    @Override
    public Operator mergeResults(List<Operator> children) {
      return planLimit(
          planOffset(
              planMerge(children)));
    }

    private boolean isLimited()
    {
      return limit < Long.MAX_VALUE;
    }

    private Operator planLimit(Operator child)
    {
      if (!isLimited()) {
        return child;
      }
      return runner.add(ScanResultLimitOperator.forQuery(queryToRun, child));
    }

    private Operator planOffset(Operator child)
    {
      if (offset == 0) {
        return child;
      }
      return runner.add(new ScanResultOffsetOperator(offset, child));
    }

    public Operator planMerge(final List<Operator> children)
    {
      return runner.add(new ConcatOperator(children));
    }
  }

  /**
   * Holds the query-dependent information needed for
   * query planning. This is information about the query as
   * a whole, after rewriting for timeout and max scatter/gather bytes.
   * <p>
   * Note that timeout is handled differently in the operator pipeline than
   * in the original query runner model. Here, the fragment context keeps track
   * of the start time and timeout: each operator just asks the context to check
   * for timeout. As a result, we don't write into the query context the timeout
   * time. That is handy, because the timeout is relative to when the query starts
   * executing, and must exclude any wait time between now and then. The fragment
   * model handles that issue for us.
   * <p>
   * The result is that the fragment runner context replaces the
   * {@link org.apache.druid.server.SetAndVerifyContextQueryRunner}.
   */
  public static class QueryPlanner<T>
  {
    final PlanContext planContext;
    final DataSourceAnalysis analysis;
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline;
    final Function<SegmentReference, SegmentReference> segmentMapFn;
    final Query<T> query;
    final Iterable<SegmentDescriptor> specs;
    final QueryRunnerFactory<T, Query<T>> factory;
    final QueryToolChest<T, Query<T>> toolChest;
    final QueryTypePlanner<T> queryTypePlanner;
    final Optional<byte[]> cacheKeyPrefix;
    final Timer waitTimer = Timer.createStarted();
    final boolean shouldFinalize;
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);
    final FragmentRunner runner;

    @SuppressWarnings("unchecked")
    public QueryPlanner(
        final PlanContext planContext,
        final Query<T> query,
        Iterable<SegmentDescriptor> specs,
        final QueryRunnerFactory<T, Query<T>> factory
    )
    {
      this.planContext = planContext;
      this.shouldFinalize = QueryContexts.isFinalize(query, true);
      this.query = planContext.withTimeoutAndMaxScatterGatherBytes(
          query.withOverriddenContext(ImmutableMap.of("finalize", false)));
      this.specs = specs;
      this.factory = factory;
      this.toolChest = factory.getToolchest();

      // Note: the timeout timer starts ticking when we start running the
      // operator pipeline, now now when we create it.
      this.runner = new FragmentRunner(
          QueryContexts.getTimeout(this.query)
          );

      // TODO: Obtain the planner from the factory
      Preconditions.checkArgument(((Object) factory) instanceof ScanQueryRunnerFactory);
      this.queryTypePlanner = (QueryTypePlanner<T>)
          new ScanQueryPlanner(toolChest, runner);

      this.analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
      final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline =
          planContext.segmentManager.getTimeline(analysis);

      // Make sure this query type can handle the subquery, if present.
      if (analysis.isQuery() && !toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery())) {
        throw new ISE("Cannot handle subquery: %s", analysis.getDataSource());
      }

      if (!maybeTimeline.isPresent()) {
        // Query will be planned as just a report missing timeline operator

        this.timeline = null;
        this.segmentMapFn = null;
        this.cacheKeyPrefix = null;
        return;
      }
      this.timeline = maybeTimeline.get();

      // segmentMapFn maps each base Segment into a joined Segment if necessary.
      this.segmentMapFn = planContext.joinableFactoryWrapper.createSegmentMapFn(
          analysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null),
          analysis.getPreJoinableClauses(),
          cpuTimeAccumulator,
          analysis.getBaseQuery().orElse(query)
      );

      // We compute the join cache key here itself so it doesn't need to be re-computed for every segment
      this.cacheKeyPrefix = analysis.isJoin()
          ? planContext.joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis)
          : Optional.of(StringUtils.EMPTY_BYTES);
    }

    Operator add(Operator op)
    {
      return runner.add(op);
    }

    public QueryRunner<T> planQueryStub()
    {
      planQuery();
      // Convert the fragment runner to a query runner.
      return runner.toRunner();
    }

    Operator planQuery()
    {
      List<SegmentDescriptor> descriptors = Lists.newArrayList(specs);
      if (timeline == null) {
        return add(
            new MissingSegmentsOperator(descriptors));
      }
      Query<T> rewrittenQuery = queryTypePlanner.prepareMerge(query);
      // TODO: Avoid materializing the segment until needed, as in original code
      List<Operator> segOps = descriptors.stream()
          .map(descriptor -> planSegment(rewrittenQuery, descriptor))
          .collect(Collectors.toList());

      return planFinalizeResults(
                planMerge(segOps));
//      return planCpuMetric(
//          planFinalizeResults(
//              planMerge(segOps));
      // merge
      // finalize
      // time
    }

    /**
     * Create the plan for a single segment using a dedicated segment planner.
     */
    private Operator planSegment(Query<T> rewrittenQuery, SegmentDescriptor descriptor)
    {
      SegmentPlanner<T> segmentPlanner = new SegmentPlanner<>(
          this,
          rewrittenQuery,
          descriptor
          );

       return segmentPlanner.plan();
    }

//    private Operator planCpuMetric(Operator child)
//    {
//      return runner.add(new CPUMetricOperator(child));
//    }

    /**
     * Plan step that applies {@link QueryToolChest#makePostComputeManipulatorFn(Query, MetricManipulationFn)} to the
     * result stream. It is expected to be the last operator in the pipeline, after results are fully merged.
     * <p>
     * Note that despite the type parameter "T", this runner may not actually return sequences with type T. This most
     * commonly happens when an upstream {@link BySegmentQueryRunner} changes the result stream to type
     * {@code Result<BySegmentResultValue<T>>}, in which case this class will retain the structure, but call the finalizer
     * function on each result in the by-segment list (which may change their type from T to something else).
     *
     * @see {@link org.apache.druid.query.FinalizeResultsQueryRunner}
     */
    private Operator planFinalizeResults(final Operator child)
    {
      final MetricManipulationFn metricManipulationFn;
      if (shouldFinalize) {
        metricManipulationFn = MetricManipulatorFns.finalizing();
      } else {
        metricManipulationFn = MetricManipulatorFns.identity();
      }
      final com.google.common.base.Function<T, T> baseFinalizer = toolChest.makePostComputeManipulatorFn(
          query,
          metricManipulationFn
          );
      final com.google.common.base.Function<T, ?> finalizerFn;
      if (QueryContexts.isBySegment(query)) {
        finalizerFn = new com.google.common.base.Function<T, Result<BySegmentResultValue<T>>>()
        {
          @Override
          public Result<BySegmentResultValue<T>> apply(T input)
          {
            //noinspection unchecked (input is not actually a T; see class-level javadoc)
            @SuppressWarnings("unchecked")
            Result<BySegmentResultValueClass<T>> result = (Result<BySegmentResultValueClass<T>>) input;

            if (input == null) {
              throw new ISE("Cannot have a null result!");
            }

            BySegmentResultValue<T> resultsClass = result.getValue();

            return new Result<>(
                result.getTimestamp(),
                new BySegmentResultValueClass<>(
                    Lists.transform(resultsClass.getResults(), baseFinalizer),
                    resultsClass.getSegmentId(),
                    resultsClass.getInterval()
                )
            );
          }
        };
      } else {
        // Optimize away the finalizer operator if nothing to do.
        if (baseFinalizer == Functions.identity())
        {
          return child;
        }
        finalizerFn = baseFinalizer;
      }
      return runner.add(new TransformOperator(finalizerFn, child));
    }

    private Operator planMerge(List<Operator> segOps)
    {
      return queryTypePlanner.mergeResults(segOps);
    }
  }

  /**
   * Holds query- and fragment-dependent information needed
   * to plan the per-fragment parts of a query.
   * <p>
   * This planner optimizes queries made on a single segment, using per-segment information,
   * before submitting the queries to the base runner.
   * <p>
   * Example optimizations include adjusting query filters based on per-segment information, such as intervals.
   * <p>
   * This planner plans queries that will
   * be used to query a single segment (i.e., when the query reaches a historical node).
   * <p>
   *
   * @see {@link org.apache.druid.query.PerSegmentOptimizingQueryRunner}
   */
  public static class SegmentPlanner<T>
  {
    final PlanContext planContext;
    final QueryPlanner<T> queryPlanner;
    final Query<T> query;
    final SegmentDescriptor segmentDescriptor;
    final QueryMetrics<?> queryMetrics;
    Interval actualDataInterval;
    SegmentReference segment;
    String segmentIdString;

    public SegmentPlanner(
        final QueryPlanner<T> queryPlanner,
        final Query<T> query,
        final SegmentDescriptor descriptor
        )
    {
      this.planContext = queryPlanner.planContext;
      this.queryPlanner = queryPlanner;

      // See PerSegmentOptimizingQueryRunner
      // Can be done here because this is the planner for a per-segment query.
      // See SpecificSegmentQueryRunner
      this.query = Queries.withSpecificSegments(
              query,
              Collections.singletonList(descriptor)
              ).optimizeForSegment(
                  new PerSegmentQueryOptimizationContext(descriptor));

      this.segmentDescriptor = descriptor;
      final QueryPlus<T> queryWithMetrics = QueryPlus.wrap(query).withQueryMetrics(queryPlanner.toolChest);
      this.queryMetrics = queryWithMetrics.getQueryMetrics();
    }

    public Operator plan()
    {
      final PartitionChunk<ReferenceCountingSegment> chunk = queryPlanner.timeline.findChunk(
          segmentDescriptor.getInterval(),
          segmentDescriptor.getVersion(),
          segmentDescriptor.getPartitionNumber()
      );

      if (chunk == null) {
        return queryPlanner.add(
            new MissingSegmentsOperator(segmentDescriptor));
      }

      final ReferenceCountingSegment segmentChunk = chunk.getObject();
      segment = queryPlanner.segmentMapFn.apply(segmentChunk);

      StorageAdapter storageAdapter = segment.asStorageAdapter();
      long segmentMaxTime = storageAdapter.getMaxTime().getMillis();
      long segmentMinTime = storageAdapter.getMinTime().getMillis();
      actualDataInterval = Intervals.utc(segmentMinTime, segmentMaxTime + 1);

      // See ServerManager.buildQueryRunnerForSegment
      final SegmentId segmentId = segment.getId();
      final Interval segmentInterval = segment.getDataInterval();
      // SegmentLockOperator will return null for ID or interval if it's already closed.
      // Here, we check one more time if the segment is closed.
      // If the segment is closed after this line, SegmentLockOperator will handle and do the right thing.
      if (segmentId == null || segmentInterval == null) {
        return queryPlanner.add(
            new MissingSegmentsOperator(segmentDescriptor));
      }
      segmentIdString = segmentId.toString();
      return planSegment();
   }

    /**
     * Plan a per-segment query.
     * <p>
     * For the most part, each query runner in the traditional implementation maps to
     * an operator in the operator pipeline. Some operators have effect only in their
     * start and end to mimic wrapper sequences.
     * <p>
     * One exception is the {@code PerSegmentOptimizingQueryRunner}, which simply rewrites
     * the query and is done in the constructor above. Unlike with query runners, operators
     * don't pass a query downwards, so we do the work here instead.
     * <p>
     * Another exception is {@code SetAndVerifyContextQueryRunner} which sets timeouts.
     * Timeout in this model is handled by the fragment runner, so no need for an operator
     * to rewrite the query for timeout.
     *
     * @see {@link org.apache.druid.server.coordination.ServerManager#buildAndDecorateQueryRunner}
     */
    public Operator planSegment()
    {

      return planSpecificSegment(
          planSegmentAndCacheMetrics(
            planBySegment(
              planCache(
                planSegmentMetrics(
                  planRefCount(
                      planScan()))))));
    }

    /**
     * @see {@link org.apache.druid.query.spec.SpecificSegmentQueryRunner}
     */
    private Operator planSpecificSegment(Operator child)
    {
      final String newName = query.getType() + "_" + query.getDataSource() + "_" + query.getIntervals();
      return queryPlanner.add(new ThreadLabelOperator(newName, child));
    }

    private Operator planSegmentAndCacheMetrics(Operator child)
    {
      return planMetrics(
          QueryMetrics::reportSegmentAndCacheTime,
          true,
          child
          );
    }

    /**
     * Create an operator that wraps a base single-segment query operator, and wraps its results in a
     * {@link BySegmentResultValueClass} object if the "bySegment" query context parameter is set. Otherwise, it
     * delegates to the base operator without any behavior modification.
     *
     * @see {@link org.apache.druid.server.coordination.ServerManager#buildAndDecorateQueryRunner}
     * @see {@link org.apache.druid.query.BySegmentQueryRunner}
     */
    private Operator planBySegment(final Operator child)
    {
      if (!QueryContexts.isBySegment(query)) {
        return child;
      }
      // By-segment is not compatible with a scan query.
      // (See Issue #11862: https://github.com/apache/druid/issues/11862)
      // So, ignore this option for scan queries.
      if (query instanceof ScanQuery) {
        return child;
      }
      return queryPlanner.add(
          new BySegmentOperator(
              segmentIdString,
              segment.getDataInterval().getStart(),
              query.getIntervals().get(0),
              child));
    }

    /**
     * Plan the cache operation. Depending on configuration, the query may
     * come from, or go to the cache (or both or neither). The to-cache and
     * from-cache steps are distinct operators. We add only those requested
     * by configuration. This means the operator DAG will not contain cache
     * operations if caching is not enabled.
     *
     * @see {@link org.apache.druid.client.CachingQueryRunner}
     */
    private Operator planCache(final Operator child)
    {
      if (!queryPlanner.cacheKeyPrefix.isPresent()) {
        return child;
      }
      final CacheStrategy<T, Object, Query<T>> strategy = queryPlanner.toolChest.getCacheStrategy(query);
      final boolean populate = canPopulateCache(strategy);
      final boolean use = canUseCache(strategy);
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

    private Operator planSegmentMetrics(Operator child)
    {
      return planMetrics(
          QueryMetrics::reportSegmentTime,
          false,
          child
          );
    }

    /**
     * Add the metrics-reporting operator. Allows metrics to be optional. If not
     * present, simply omits the metrics operator.
     *
     * @see {@link org.apache.druid.server.coordination.ServerManager#buildAndDecorateQueryRunner}
     */
    private Operator planMetrics(
        ObjLongConsumer<? super QueryMetrics<?>> reportMetric,
        boolean withWaitTime,
        Operator child)
    {
      if (queryMetrics == null) {
        return child;
      }
      return queryPlanner.add(
          new MetricsOperator(
              planContext.emitter,
              segment.getId().toString(),
              queryMetrics,
              reportMetric,
              // TODO: Should wait time be reported at the segment level?
              // TODO: Should wait time be from the start of query?
              withWaitTime ? Timer.createStarted() : null,
              child));
    }

    /**
     * @see {@link org.apache.druid.server.coordination.ServerManager#buildAndDecorateQueryRunner}
     */
    private Operator planRefCount(Operator child)
    {
      return queryPlanner.add(new SegmentLockOperator(segment, segmentDescriptor, child));
    }

    private Operator planScan()
    {
      return queryPlanner.add(queryPlanner.queryTypePlanner.planScan(query, segment));
    }

    /**
     * @return whether the segment level cache should be used or not. False if strategy is null
     * @see {@link org.apache.druid.client.CachingQueryRunner#canUseCache}
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
     * @see {@link org.apache.druid.client.CachingQueryRunner#canPopulateCache}
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

    /**
     * @see {@link org.apache.druid.client.CachingQueryRunner#alignToActualDataInterval}
     */
    private SegmentDescriptor alignToActualDataInterval()
    {
      Interval interval = segmentDescriptor.getInterval();
      return new SegmentDescriptor(
          interval.overlaps(actualDataInterval) ? interval.overlap(actualDataInterval) : interval,
          segmentDescriptor.getVersion(),
          segmentDescriptor.getPartitionNumber()
      );
    }
  }

//  public static <T> QueryRunner<T> planSegmentStub(
//      final QueryPlanner<T> queryPlanner,
//      final Query<T> query,
//      final SegmentDescriptor descriptor
//  )
//  {
//    SegmentPlanner<T> segmentPlanner = new SegmentPlanner<>(
//        queryPlanner,
//        query,
//        descriptor
//        );
//
//    // Ignore the root, we'll get it from the runner.
//    segmentPlanner.plan();
//
//    // Convert the fragment runner to a query runner.
//    return queryPlanner.runner.toRunner();
//  }
}
