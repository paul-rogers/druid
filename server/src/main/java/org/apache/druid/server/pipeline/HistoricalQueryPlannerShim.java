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

import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.ServerManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.pipeline.HistoricalQueryPlannerStub.PlanContext;
import org.apache.druid.server.pipeline.HistoricalQueryPlannerStub.QueryPlanner;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Temporary query planner that combines QueryRunners for the "top" part
 * of the query, with operators for the "bottom" part.
 */
public class HistoricalQueryPlannerShim
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final QueryProcessingPool queryProcessingPool;
  private final CachePopulator cachePopulator;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;
  private final SegmentManager segmentManager;
  private final JoinableFactoryWrapper joinableFactoryWrapper;
  private final ServerConfig serverConfig;

  public HistoricalQueryPlannerShim(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      QueryProcessingPool queryProcessingPool,
      CachePopulator cachePopulator,
      ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager,
      JoinableFactoryWrapper joinableFactoryWrapper,
      ServerConfig serverConfig
  )
  {
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.queryProcessingPool = queryProcessingPool;
    this.cachePopulator = cachePopulator;
    this.cache = cache;
    this.objectMapper = objectMapper;

    this.cacheConfig = cacheConfig;
    this.segmentManager = segmentManager;
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.serverConfig = serverConfig;
  }

  public <T> QueryRunner<T> plan(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      final QueryUnsupportedException e = new QueryUnsupportedException(
          StringUtils.format("Unknown query type, [%s]", query.getClass())
      );
      log.makeAlert(e, "Error while executing a query[%s]", query.getId())
         .addData("dataSource", query.getDataSource())
         .emit();
      throw e;
    }

//    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
////    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
//    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);
//
//    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline;
//    final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline =
//        segmentManager.getTimeline(analysis);

//    // Make sure this query type can handle the subquery, if present.
//    if (analysis.isQuery() && !toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery())) {
//      throw new ISE("Cannot handle subquery: %s", analysis.getDataSource());
//    }

//    if (maybeTimeline.isPresent()) {
//      timeline = maybeTimeline.get();
//    } else {
//      return new ReportTimelineMissingSegmentQueryRunner<>(Lists.newArrayList(specs));
//    }

//    // segmentMapFn maps each base Segment into a joined Segment if necessary.
//    final Function<SegmentReference, SegmentReference> segmentMapFn = joinableFactoryWrapper.createSegmentMapFn(
//        analysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null),
//        analysis.getPreJoinableClauses(),
//        cpuTimeAccumulator,
//        analysis.getBaseQuery().orElse(query)
//    );
//    // We compute the join cache key here itself so it doesn't need to be re-computed for every segment
//    final Optional<byte[]> cacheKeyPrefix = analysis.isJoin()
//                                            ? joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis)
//                                            : Optional.of(StringUtils.EMPTY_BYTES);

    // One of these per engine. Created in-line for now.
    final PlanContext planContext = new PlanContext(
        serverConfig,
        emitter,
        cacheConfig,
        cache,
        cachePopulator,
        objectMapper,
        segmentManager,
        joinableFactoryWrapper
        );
    // One per query.
    final QueryPlanner<T> queryPlanner = new QueryPlanner<>(
        planContext,
        query,
        specs,
        factory
        );
    return queryPlanner.planQueryStub();
//    final FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
//        .create(specs)
//        .transformCat(
//            descriptor -> Collections.singletonList(
//                buildQueryRunnerForSegment(
//                    query,
//                    descriptor,
////                    factory,
////                    toolChest,
//                    timeline,
//                    segmentMapFn,
////                    cpuTimeAccumulator,
////                    cacheKeyPrefix,
//                    queryPlanner
//                )
//            )
//        );

//    return CPUTimeMetricQueryRunner.safeBuild(
//        stubRunner,
////        new FinalizeResultsQueryRunner<>(
////            stubRunner,
//////            toolChest.mergeResults(factory.mergeRunners(queryProcessingPool, queryRunners)),
////            toolChest
////        ),
//        toolChest,
//        emitter,
//        cpuTimeAccumulator,
//        true
//    );
  }

//  protected <T> QueryRunner<T> buildQueryRunnerForSegment(
//      final Query<T> query,
//      final SegmentDescriptor descriptor,
////      final QueryRunnerFactory<T, Query<T>> factory,
////      final QueryToolChest<T, Query<T>> toolChest,
//      final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline,
//      final Function<SegmentReference, SegmentReference> segmentMapFn,
////      final AtomicLong cpuTimeAccumulator,
////      Optional<byte[]> cacheKeyPrefix,
//      final QueryPlanner<T> queryPlanner
//  )
//  {
//    final PartitionChunk<ReferenceCountingSegment> chunk = timeline.findChunk(
//        descriptor.getInterval(),
//        descriptor.getVersion(),
//        descriptor.getPartitionNumber()
//    );
//
//    if (chunk == null) {
//      return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
//    }
//
//    final ReferenceCountingSegment segment = chunk.getObject();
//    return HistoricalQueryPlannerStub.planSegmentStub(
//        queryPlanner,
//        query,
//        descriptor);
////    return buildAndDecorateQueryRunner(
////        query,
////        factory,
////        toolChest,
////        segmentMapFn.apply(segment),
////        cacheKeyPrefix,
////        descriptor,
////        cpuTimeAccumulator,
////        queryPlanner
////    );
//  }

//  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
//      final Query<T> query,
//      final QueryRunnerFactory<T, Query<T>> factory,
//      final QueryToolChest<T, Query<T>> toolChest,
//      final SegmentReference segment,
//      final Optional<byte[]> cacheKeyPrefix,
//      final SegmentDescriptor segmentDescriptor,
//      final AtomicLong cpuTimeAccumulator,
//      final QueryPlanner<T> queryPlanner
//  )
//  {
//    final SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
//    final SegmentId segmentId = segment.getId();
//    final Interval segmentInterval = segment.getDataInterval();
//    // ReferenceCountingSegment can return null for ID or interval if it's already closed.
//    // Here, we check one more time if the segment is closed.
//    // If the segment is closed after this line, ReferenceCountingSegmentQueryRunner will handle and do the right thing.
//    if (segmentId == null || segmentInterval == null) {
//      return new ReportTimelineMissingSegmentQueryRunner<>(segmentDescriptor);
//    }
//    String segmentIdString = segmentId.toString();

//    return HistoricalQueryPlannerStub.planSegmentStub(
//        queryPlanner,
//        query,
//        segment,
//        segmentDescriptor);

//    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerInner = new MetricsEmittingQueryRunner<>(
//        emitter,
//        toolChest,
//        stubRunner,
//        //new ReferenceCountingSegmentQueryRunner<>(factory, segment, segmentDescriptor),
//        QueryMetrics::reportSegmentTime,
//        queryMetrics -> queryMetrics.segment(segmentIdString)
//    );

//    StorageAdapter storageAdapter = segment.asStorageAdapter();
//    long segmentMaxTime = storageAdapter.getMaxTime().getMillis();
//    long segmentMinTime = storageAdapter.getMinTime().getMillis();
//    Interval actualDataInterval = Intervals.utc(segmentMinTime, segmentMaxTime + 1);
//    CachingQueryRunner<T> cachingQueryRunner = new CachingQueryRunner<>(
//        segmentIdString,
//        cacheKeyPrefix,
//        segmentDescriptor,
//        actualDataInterval,
//        objectMapper,
//        cache,
//        toolChest,
//        stubRunner,
//        cachePopulator,
//        cacheConfig
//    );

//    BySegmentQueryRunner<T> bySegmentQueryRunner = new BySegmentQueryRunner<>(
//        segmentId,
//        segmentInterval.getStart(),
//        stubRunner
//    );

//    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerOuter = new MetricsEmittingQueryRunner<>(
//        emitter,
//        toolChest,
//        stubRunner,
//        QueryMetrics::reportSegmentAndCacheTime,
//        queryMetrics -> queryMetrics.segment(segmentIdString)
//    ).withWaitMeasuredFromNow();

//    SpecificSegmentQueryRunner<T> specificSegmentQueryRunner = new SpecificSegmentQueryRunner<>(
//        stubRunner,
//        segmentSpec
//    );

//    PerSegmentOptimizingQueryRunner<T> perSegmentOptimizingQueryRunner = new PerSegmentOptimizingQueryRunner<>(
//        stubRunner,
//        new PerSegmentQueryOptimizationContext(segmentDescriptor)
//    );

//    return new SetAndVerifyContextQueryRunner<>(
//        serverConfig,
//        CPUTimeMetricQueryRunner.safeBuild(
//            stubRunner,
//            toolChest,
//            emitter,
//            cpuTimeAccumulator,
//            false
//        )
//    );
//  }
}
