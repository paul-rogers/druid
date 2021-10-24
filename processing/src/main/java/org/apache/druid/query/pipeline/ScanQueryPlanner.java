package org.apache.druid.query.pipeline;

import java.util.List;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.ConcatOperator.Defn;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryOffsetSequence;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Segment;

import com.google.inject.Inject;

/**
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory}
 * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest}
 */
public class ScanQueryPlanner
{
  private final ScanQueryQueryToolChest toolChest;

  @Inject
  public ScanQueryPlanner(final ScanQueryQueryToolChest toolChest)
  {
    this.toolChest = toolChest;
  }

  public OperatorDefn plan(QueryPlus<ScanResultValue> queryPlus)
  {
    Query<ScanResultValue> query = queryPlus.getQuery();
    if (!(query instanceof ScanQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
    }
    return planScanQuery((ScanQuery) query);
  }

  // See ScanQueryQueryToolChest.mergeResults
  public OperatorDefn planScanQuery(ScanQuery query)
  {
    // Ensure "legacy" is a non-null value, such that all other nodes this query is forwarded to will treat it
    // the same way, even if they have different default legacy values.
    query = query.withNonNullLegacy(toolChest.getConfig());
    return planOffset(query);
  }

  // See ScanQueryQueryToolChest.mergeResults
  public OperatorDefn planOffset(ScanQuery query)
  {
    long offset = query.getScanRowsOffset();
    if (offset == 0) {
      return planLimit(query);
    }
    // Remove "offset" and add it to the "limit" (we won't push the offset down, just apply it here, at the
    // merge at the top of the stack).
    long limit;
    if (!query.isLimited()) {
      // Unlimited stays unlimited.
      limit = Long.MAX_VALUE;
    } else {
      limit = query.getScanRowsLimit();
      if (limit > Long.MAX_VALUE - offset) {
        throw new ISE(
            "Cannot apply limit[%d] with offset[%d] due to overflow",
            limit,
            offset
        );
      }
      limit += offset;
    }
    query = query
        .withOffset(0)
        .withLimit(limit);
    return planLimit(query);
  }

  // See ScanQueryQueryToolChest.mergeResults
  public OperatorDefn planLimit(ScanQuery query)
  {
    OperatorDefn child = unknown(query);
    if (!query.isLimited()) {
      return child;
    }
    return new ScanResultLimitOperator.Defn(query, child);
  }

  // See ScanQueryRunnerFactory.mergeRunners
  public OperatorDefn planMerge(ScanQuery query)
  {
    List<OperatorDefn> children = unknownList(query);
    if (query.getOrder().equals(ScanQuery.Order.NONE)) {
      // Use normal strategy
      OperatorDefn concat = new ConcatOperator.Defn(children);
      if (query.isLimited()) {
        return new ScanResultLimitOperator.Defn(query, concat);
      }
      return concat;
    }
    return null;
  }

  public OperatorDefn unknown(ScanQuery query) {
    return null;
  }

  public List<OperatorDefn> unknownList(ScanQuery query) {
    return null;
  }

  // See ScanQueryRunnerFactory.ScanQueryRunner.run
  public OperatorDefn planScan(ScanQuery query, Segment segment) {
    return new ScanQueryOperator.Defn(query, segment);
  }
}
