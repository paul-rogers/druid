package org.apache.druid.query.pipeline;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Segment;

import com.google.common.collect.ImmutableMap;

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
      return ScanPlanner.runMergeResults(queryPlus, input, responseContext, scanQueryConfig);
    }
  }

  /**
   * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest.mergeResults}
   */
  public static Sequence<ScanResultValue> runMergeResults(
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

    final Sequence<ScanResultValue> results;

    if (!queryToRun.isLimited()) {
      results = input.run(queryPlus.withQuery(queryToRun), responseContext);
    } else {
      results = limit(queryPlus.withQuery(queryToRun), input, responseContext);
    }

    if (originalQuery.getScanRowsOffset() > 0) {
      return offset(results, originalQuery, responseContext);
    } else {
      return results;
    }
  }

  /**
   * Shim function to convert from query runner format to create the limit
   * operator.
   *
   * @see {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator}
   */
  private static Sequence<ScanResultValue> limit(
      final QueryPlus<ScanResultValue> inputQuery,
      QueryRunner<ScanResultValue> input,
      final ResponseContext responseContext
  )
  {
    ScanQuery query = (ScanQuery) inputQuery.getQuery();
    ScanQuery.ResultFormat resultFormat = query.getResultFormat();
    if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
      throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
    }
    boolean grouped = query.getOrder() == ScanQuery.Order.NONE ||
        !query.getContextBoolean(ScanQuery.CTX_KEY_OUTERMOST, true);
    Query<ScanResultValue> historicalQuery =
        inputQuery.getQuery().withOverriddenContext(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false));
    ScanResultLimitOperator op = new ScanResultLimitOperator(
        query.getScanRowsLimit(),
        grouped,
        query.getBatchSize(),
        Operators.toProducer(input, QueryPlus.wrap(historicalQuery), responseContext)
        );
    responseContext.getFragmentContext().register(op);
    return QueryPlanner.toSequence(op, query, responseContext);
  }

  /**
   * Shim function to convert from the query runner protocol to the offset
   * operator protocol.
   */
  private static Sequence<ScanResultValue> offset(
      Sequence<ScanResultValue> baseSequence,
      ScanQuery query,
      final ResponseContext responseContext)
  {
    ScanResultOffsetOperator op = new ScanResultOffsetOperator(
        query.getScanRowsOffset(),
        Operators.toProducer(baseSequence)
        );
    responseContext.getFragmentContext().register(op);
    return QueryPlanner.toSequence(op, query, responseContext);
  }

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
