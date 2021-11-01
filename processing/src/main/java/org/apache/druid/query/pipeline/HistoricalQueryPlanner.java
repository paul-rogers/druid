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

import java.util.Optional;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableList;

/**
 * Planner for a query running on a historical node. Such queries are a combination
 * of generic steps done for all queries, and steps done for specific query types.
 *
 * @see {@link org.apache.druid.server.coordination.ServerManager}
 */
public class HistoricalQueryPlanner<T>
{
  private static final Logger LOG = new Logger(ReportTimelineMissingSegmentQueryRunner.class);

  private final ServiceEmitter emitter;
  final QueryRunnerFactory<T, Query<T>> factory;
  final QueryToolChest<T, Query<T>> toolChest;
  final SegmentReference segment;
  final Optional<byte[]> cacheKeyPrefix;
  final SegmentDescriptor segmentDescriptor;

  public HistoricalQueryPlanner(
      final ServiceEmitter emitter,
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final SegmentReference segment,
      final Optional<byte[]> cacheKeyPrefix,
      final SegmentDescriptor segmentDescriptor
  )
  {
    this.emitter = emitter;
    this.factory = factory;
    this.toolChest = toolChest;
    this.segment = segment;
    this.cacheKeyPrefix = cacheKeyPrefix;
    this.segmentDescriptor = segmentDescriptor;
  }

  /**
   * @see {@link org.apache.druid.server.coordination.ServerManager#buildAndDecorateQueryRunner}
   * @return definition of the root operator
   */
  public Operator plan(final QueryPlus<T> queryPlus)
  {
    final SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
    final SegmentId segmentId = segment.getId();
    final Interval segmentInterval = segment.getDataInterval();
    // ReferenceCountingSegment can return null for ID or interval if it's already closed.
    // Here, we check one more time if the segment is closed.
    // If the segment is closed after this line, ReferenceCountingSegmentQueryRunner will handle and do the right thing.
    if (segmentId == null || segmentInterval == null) {
      return new MissingSegmentsOperator(ImmutableList.of(segmentDescriptor));
    }
    return null;
  }

  public Operator planSegmentLock(
      final SegmentReference segment,
      final SegmentDescriptor segmentDescriptor,
      final Operator child
  )
  {
    return new SegmentLockOperator(segment, segmentDescriptor, child);
  }

  public Operator planMetrics(final QueryPlus<T> queryPlus, final Operator child)
  {
    final QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(toolChest);
    final QueryMetrics<?> queryMetrics = queryWithMetrics.getQueryMetrics();

    return new MetricsOperator(
        emitter,
        segment.getId().toString(),
        queryMetrics,
        QueryMetrics::reportSegmentTime,
        null,
        child
        );
  }
}
