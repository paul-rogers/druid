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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Segment;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Scan-specific parts of the hybrid query planner.
 *
 * @see {@link QueryPlanner}
 */
public class ScanPlanner
{
  /**
   * Query runner which implements the
   * {@link org.apache.druid.query.scan.ScanQueryQueryToolChest ScanQueryQueryToolChest}
   * {@code mergeResults()} operation in terms of operators.
   * <p>
   * The basic code is a clone of that in {@code ScanQueryQueryToolChest}. if all works
   * fine, we'd remove the original code in favor of this implementation.
   * <p>
   * Operators perform the transform steps. Each takes a sequence as input and produces
   * a sequence as output. In both cases, a wrapper class does the deed. However, if the
   * query has both a limit and an offset, then the intermediate layers are stripped
   * away to leave just {@code offset --> limit} directly.
   *
   * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest ScanQueryQueryToolChest#mergeResults}
   */
  public static class LimitAndOffsetRunner implements QueryRunner<ScanResultValue>
  {
    private final ScanQueryConfig scanQueryConfig;
    private final QueryRunner<ScanResultValue> input;

    public LimitAndOffsetRunner(
        final ScanQueryConfig scanQueryConfig,
        final QueryRunner<ScanResultValue> input
    )
    {
      this.scanQueryConfig = scanQueryConfig;
      this.input = input;
    }

    /**
     * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest.mergeResults}
     */
    @Override
    public Sequence<ScanResultValue> run(
        final QueryPlus<ScanResultValue> queryPlus,
        final ResponseContext responseContext)
    {
      return ScanPlanner.runLimitAndOffset(queryPlus, input, responseContext, scanQueryConfig);
    }
  }

  /**
   * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest.mergeResults}
   */
  public static Sequence<ScanResultValue> runLimitAndOffset(
      final QueryPlus<ScanResultValue> queryPlus,
      QueryRunner<ScanResultValue> input,
      final ResponseContext responseContext,
      ScanQueryConfig scanQueryConfig)
  {
    // Ensure "legacy" is a non-null value, such that all other nodes this query is forwarded to will treat it
    // the same way, even if they have different default legacy values.
    //
    // Also, remove "offset" and add it to the "limit" (we won't push the offset down, just apply it here, at the
    // merge at the top of the stack).
    final ScanQuery originalQuery = ((ScanQuery) (queryPlus.getQuery()));

    final long newLimit;
    if (!originalQuery.isLimited()) {
      // Unlimited stays unlimited.
      newLimit = Long.MAX_VALUE;
    } else if (originalQuery.getScanRowsLimit() > Long.MAX_VALUE - originalQuery.getScanRowsOffset()) {
      throw new ISE(
          "Cannot apply limit[%d] with offset[%d] due to overflow",
          originalQuery.getScanRowsLimit(),
          originalQuery.getScanRowsOffset()
      );
    } else {
      newLimit = originalQuery.getScanRowsLimit() + originalQuery.getScanRowsOffset();
    }

    final ScanQuery queryToRun = originalQuery.withNonNullLegacy(scanQueryConfig)
                                              .withOffset(0)
                                              .withLimit(newLimit);

    final boolean hasLimit = queryToRun.isLimited();
    final boolean hasOffset = originalQuery.getScanRowsOffset() > 0;

    // Short-circuit if no limit or offset.
    if (!hasLimit && !hasOffset) {
      return input.run(queryPlus.withQuery(queryToRun), responseContext);
    }

    Query<ScanResultValue> historicalQuery = queryToRun;
    if (hasLimit) {
      ScanQuery.ResultFormat resultFormat = queryToRun.getResultFormat();
      if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
        throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
      }
      historicalQuery =
          queryToRun.withOverriddenContext(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false));
    }
    Operator inputOp = Operators.toOperator(
        input,
        QueryPlus.wrap(historicalQuery));
    if (hasLimit) {
      final ScanQuery limitedQuery = (ScanQuery) historicalQuery;
      ScanResultLimitOperator op = new ScanResultLimitOperator(
          limitedQuery.getScanRowsLimit(),
          isGrouped(queryToRun),
          limitedQuery.getBatchSize(),
          inputOp
          );
      inputOp = op;
    }
    if (hasOffset) {
      ScanResultOffsetOperator op = new ScanResultOffsetOperator(
          queryToRun.getScanRowsOffset(),
          inputOp
          );
      inputOp = op;
    }
    return QueryPlanner.toSequence(inputOp, historicalQuery, responseContext);
  }

  private static boolean isGrouped(ScanQuery query)
  {
    return query.getOrder() == ScanQuery.Order.NONE ||
        !query.getContextBoolean(ScanQuery.CTX_KEY_OUTERMOST, true);
  }

  /**
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory.mergeRunners}
   */
  private static Sequence<ScanResultValue> runConcatMerge(
      final QueryPlus<ScanResultValue> queryPlus,
      final Iterable<QueryRunner<ScanResultValue>> queryRunners,
      final ResponseContext responseContext)
  {
    List<Operator> inputs = new ArrayList<>();
    for (QueryRunner<ScanResultValue> qr : queryRunners) {
      inputs.add(Operators.toOperator(qr, queryPlus));
    }
    Operator op = ConcatOperator.concatOrNot(inputs);
    // TODO(paul): The original code applies a limit. Yet, when
    // run, the stack shows two limits one top of one another,
    // so the limit here seems unnecessary.
//    ScanQuery query = (ScanQuery) queryPlus.getQuery();
//    if (query.isLimited()) {
//      op = new ScanResultLimitOperator(
//          query.getScanRowsLimit(),
//          isGrouped(query),
//          query.getBatchSize(),
//          op
//          );
//    }
    return QueryPlanner.toSequence(
        op,
        queryPlus.getQuery(),
        responseContext);
  }

  /**
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory.mergeRunners}
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory.nWayMergeAndLimit}
   */
  public static Sequence<ScanResultValue> runMerge(
      final QueryPlus<ScanResultValue> queryPlus,
      final Iterable<QueryRunner<ScanResultValue>> queryRunners,
      final ResponseContext responseContext)
  {
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    // TODO(paul): timeout code

    if (query.getOrder() == ScanQuery.Order.NONE) {
      // Use normal strategy
      return runConcatMerge(
          queryPlus,
          queryRunners,
          responseContext);
    }
    return null;
//    List<Interval> intervalsOrdered = ScanQueryRunnerFactory.getIntervalsFromSpecificQuerySpec(query.getQuerySegmentSpec());
//    if (query.getOrder().equals(ScanQuery.Order.DESCENDING)) {
//      intervalsOrdered = Lists.reverse(intervalsOrdered);
//    }
//    int maxRowsQueuedForOrdering = (query.getMaxRowsQueuedForOrdering() == null
//        ? scanQueryConfig.getMaxRowsQueuedForOrdering()
//        : query.getMaxRowsQueuedForOrdering());
//    if (query.getScanRowsLimit() <= maxRowsQueuedForOrdering) {
//      // Use sort strategy
//      // TODO: Group by interval as for the n-way merge
//      return ScanResultSortOperator.forQuery(query, concat(children));
//    }
//    // Use n-way merge strategy using a priority queue
//    List<Pair<Interval, Operator>> intervalsAndRunnersOrdered = new ArrayList<>();
//    if (intervalsOrdered.size() == children.size()) {
//      for (int i = 0; i < children.size(); i++) {
//        intervalsAndRunnersOrdered.add(new Pair<>(intervalsOrdered.get(i), children.get(i)));
//      }
//      // TODO(paul): Implement this
////    } else if (queryRunners instanceof SinkQueryRunners) {
////      ((SinkQueryRunners<ScanResultValue>) queryRunners).runnerIntervalMappingIterator()
////                                                        .forEachRemaining(intervalsAndRunnersOrdered::add);
//    } else {
//      throw new ISE("Number of segment descriptors does not equal number of "
//                    + "query runners...something went wrong!");
//    }
//    // Group the list of pairs by interval.  The LinkedHashMap will have an interval paired with a list of all the
//    // operators for that segment
//    LinkedHashMap<Interval, List<Pair<Interval, Operator>>> partitionsGroupedByInterval =
//        intervalsAndRunnersOrdered.stream()
//                                  .collect(Collectors.groupingBy(
//                                      x -> x.lhs,
//                                      LinkedHashMap::new,
//                                      Collectors.toList()
//                                  ));
//
//    // Find the segment with the largest numbers of partitions.  This will be used to compare with the
//    // maxSegmentPartitionsOrderedInMemory limit to determine if the query is at risk of consuming too much memory.
//    int maxNumPartitionsInSegment =
//        partitionsGroupedByInterval.values()
//                                   .stream()
//                                   .map(x -> x.size())
//                                   .max(Comparator.comparing(Integer::valueOf))
//                                   .get();
//    int maxSegmentPartitionsOrderedInMemory = query.getMaxSegmentPartitionsOrderedInMemory() == null
//        ? scanQueryConfig.getMaxSegmentPartitionsOrderedInMemory()
//        : query.getMaxSegmentPartitionsOrderedInMemory();
//    if (maxNumPartitionsInSegment > maxSegmentPartitionsOrderedInMemory) {
//      throw ResourceLimitExceededException.withMessage(
//          "Time ordering is not supported for a Scan query with %,d segments per time chunk and a row limit of %,d. "
//          + "Try reducing your query limit below maxRowsQueuedForOrdering (currently %,d), or using compaction to "
//          + "reduce the number of segments per time chunk, or raising maxSegmentPartitionsOrderedInMemory "
//          + "(currently %,d) above the number of segments you have per time chunk.",
//          maxNumPartitionsInSegment,
//          query.getScanRowsLimit(),
//          maxRowsQueuedForOrdering,
//          maxSegmentPartitionsOrderedInMemory
//      );
//    }
//    // Use n-way merge strategy
//
//    // Create a list of grouped runner lists (i.e. each sublist/"runner group" corresponds to an interval) ->
//    // there should be no interval overlap.  We create a list of lists so we can create a sequence of sequences.
//    // There's no easy way to convert a LinkedHashMap to a sequence because it's non-iterable.
//    List<List<Operator>> groupedRunners =
//        partitionsGroupedByInterval.entrySet()
//                                   .stream()
//                                   .map(entry -> entry.getValue()
//                                                      .stream()
//                                                      .map(segQueryRunnerPair -> segQueryRunnerPair.rhs)
//                                                      .collect(Collectors.toList()))
//                                   .collect(Collectors.toList());
//
//    // Starting from the innermost map:
//    // (1) Disaggregate each ScanResultValue returned by the input operators
//    // (2) Do a n-way merge per interval group based on timestamp
//    // (3) Concatenate the groups
//    Operator result = concat(groupedRunners
//      .stream()
//      .map(group -> ScanResultMergeOperator.forQuery(
//          query,
//          group
//            .stream()
//            .map(input -> new DisaggregateScanResultOperator(input))
//            .collect(Collectors.toList())))
//      .collect(Collectors.toList()));
//
//    if (query.isLimited()) {
//      return ScanResultLimitOperator.forQuery(query, result);
//    }
//    return result;
  }

//  public Operator unknown(ScanQuery query) {
//    return null;
//  }
//
//  public List<Operator> unknownList(ScanQuery query) {
//    return null;
//  }

  /**
   * Convert the operator-based scan to that expected by the sequence-based
   * query runner.
   *
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory.ScanQueryRunner}
   */
  public static Sequence<ScanResultValue> runScan(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext)
  {
    final Number timeoutAt = (Number) responseContext.get(ResponseContext.Key.TIMEOUT_AT);
    final long timeout;
    if (timeoutAt != null && timeoutAt.longValue() > 0L) {
      timeout = timeoutAt.longValue();
    } else {
      timeout = JodaUtils.MAX_INSTANT;
    }
    // TODO (paul): Set the timeout at the overall fragment context level.
    return Operators.toSequence(
        new ScanQueryOperator(query, segment),
        new FragmentContextImpl(
            query.getId(),
            timeout,
            responseContext));
  }
}
