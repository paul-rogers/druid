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

package org.apache.druid.queryng.planner;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.general.ConcatOperator;
import org.apache.druid.queryng.operators.scan.ScanBatchToRowOperator;
import org.apache.druid.queryng.operators.scan.ScanCompactListToArrayOperator;
import org.apache.druid.queryng.operators.scan.ScanListToArrayOperator;
import org.apache.druid.queryng.operators.scan.ScanQueryOperator;
import org.apache.druid.queryng.operators.scan.ScanResultLimitOperator;
import org.apache.druid.queryng.operators.scan.ScanResultOffsetOperator;
import org.apache.druid.segment.Segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Scan-specific parts of the hybrid query planner.
 *
 * @see {@link QueryPlanner}
 */
public class ScanPlanner
{
  /**
   * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest.mergeResults}
   */
  public static Sequence<ScanResultValue> runLimitAndOffset(
      final QueryPlus<ScanResultValue> queryPlus,
      QueryRunner<ScanResultValue> input,
      final ResponseContext responseContext,
      ScanQueryConfig scanQueryConfig)
  {
    // Remove "offset" and add it to the "limit" (we won't push the offset down, just apply it here, at the
    // merge at the top of the stack).
    final ScanQuery originalQuery = ((ScanQuery) (queryPlus.getQuery()));
    ScanQuery.verifyOrderByForNativeExecution(originalQuery);

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

    // Ensure "legacy" is a non-null value, such that all other nodes this query is forwarded to will treat it
    // the same way, even if they have different default legacy values.
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
    QueryPlus<ScanResultValue> historicalQueryPlus = queryPlus.withQuery(historicalQuery);
    Operator<ScanResultValue> inputOp = Operators.toOperator(
        input,
        historicalQueryPlus);
    if (hasLimit) {
      final ScanQuery limitedQuery = (ScanQuery) historicalQuery;
      ScanResultLimitOperator op = new ScanResultLimitOperator(
          queryPlus.fragmentBuilder(),
          limitedQuery.getScanRowsLimit(),
          isGrouped(queryToRun),
          limitedQuery.getBatchSize(),
          inputOp
          );
      inputOp = op;
    }
    if (hasOffset) {
      ScanResultOffsetOperator op = new ScanResultOffsetOperator(
          queryPlus.fragmentBuilder(),
          queryToRun.getScanRowsOffset(),
          inputOp
          );
      inputOp = op;
    }
    return Operators.toSequence(inputOp);
  }

  private static boolean isGrouped(ScanQuery query)
  {
    // TODO: Review
    return query.getTimeOrder() == ScanQuery.Order.NONE ||
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
    List<Operator<ScanResultValue>> inputs = new ArrayList<>();
    for (QueryRunner<ScanResultValue> qr : queryRunners) {
      inputs.add(Operators.toOperator(qr, queryPlus));
    }
    Operator<ScanResultValue> op = ConcatOperator.concatOrNot(
        queryPlus.fragmentBuilder(),
        inputs);
    // TODO(paul): The original code applies a limit. Yet, when
    // run, the stack shows two limits one top of one another,
    // so the limit here seems unnecessary.
    // That is, we are doing a concat operation. It does not matter
    // if the limit is applied in the concat, or the next operator
    // along: in either case, we'll stop reading from upstream when the
    // limit is hit.
    //
    // ScanQuery query = (ScanQuery) queryPlus.getQuery();
    // if (query.isLimited()) {
    //   op = new ScanResultLimitOperator(
    //       query.getScanRowsLimit(),
    //       isGrouped(query),
    //       query.getBatchSize(),
    //       op
    //       );
    // }
    return Operators.toSequence(op);
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
    ScanQuery.verifyOrderByForNativeExecution(query);
    // Note: this variable is effective only when queryContext has a timeout.
    // See the comment of ResponseContext.Key.TIMEOUT_AT.
    final long timeoutAt = System.currentTimeMillis() + QueryContexts.getTimeout(queryPlus.getQuery());
    responseContext.putTimeoutTime(timeoutAt);

    // TODO: Review
    if (query.getTimeOrder() == ScanQuery.Order.NONE) {
      // Use normal strategy
      return runConcatMerge(
          queryPlus,
          queryRunners,
          responseContext);
    }
    return null;
  }

  /**
   * Convert the operator-based scan to that expected by the sequence-based
   * query runner.
   *
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory.ScanQueryRunner}
   */
  public static Sequence<ScanResultValue> runScan(
      final QueryPlus<ScanResultValue> queryPlus,
      final Segment segment,
      final ResponseContext responseContext)
  {
    if (!(queryPlus.getQuery() instanceof ScanQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", queryPlus.getQuery().getClass(), ScanQuery.class);
    }
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    ScanQuery.verifyOrderByForNativeExecution((ScanQuery) query);
    final Long timeoutAt = responseContext.getTimeoutTime();
    if (timeoutAt == null || timeoutAt == 0L) {
      responseContext.putTimeoutTime(JodaUtils.MAX_INSTANT);
    }
    // TODO (paul): Set the timeout at the overall fragment context level.
    return Operators.toSequence(
        new ScanQueryOperator(
            queryPlus.fragmentBuilder(),
            query,
            segment));
  }

  public static Sequence<Object[]> resultsAsArrays(
      QueryPlus<ScanResultValue> queryPlus,
      final List<String> fields,
      final Sequence<ScanResultValue> resultSequence)
  {
    Operator<ScanResultValue> inputOp = Operators.toOperator(
        queryPlus.fragmentBuilder(),
        resultSequence);
    Operator<Object[]> outputOp;
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    switch (query.getResultFormat()) {
      case RESULT_FORMAT_LIST:
        outputOp = new ScanListToArrayOperator(
            queryPlus.fragmentBuilder(),
            new ScanBatchToRowOperator<Map<String, Object>>(
                queryPlus.fragmentBuilder(),
                inputOp),
            fields);
        break;
      case RESULT_FORMAT_COMPACTED_LIST:
        outputOp = new ScanCompactListToArrayOperator(
            queryPlus.fragmentBuilder(),
            new ScanBatchToRowOperator<List<Object>>(
                queryPlus.fragmentBuilder(),
                inputOp),
            fields);
        break;
      default:
        throw new UOE("Unsupported resultFormat for array-based results: %s", query.getResultFormat());
    }
    return Operators.toSequence(outputOp);
  }
}
