package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.SinkQueryRunners;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.ConcatOperator.Defn;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine2;
import org.apache.druid.query.scan.ScanQueryOffsetSequence;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Segment;
import org.joda.time.Interval;

import com.google.common.collect.Lists;
import com.google.inject.Inject;

/**
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory}
 * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest}
 */
public class ScanQueryPlanner
{
  private final ScanQueryQueryToolChest toolChest;
  private final ScanQueryConfig scanQueryConfig;

  @Inject
  public ScanQueryPlanner(
      ScanQueryQueryToolChest toolChest,
      ScanQueryConfig scanQueryConfig
  )
  {
    this.toolChest = toolChest;
    this.scanQueryConfig = scanQueryConfig;
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
    query = query.withNonNullLegacy(scanQueryConfig);
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
  // See ScanQueryRunnerFactory.nWayMergeAndLimit
  public OperatorDefn planMerge(ScanQuery query)
  {
    List<Interval> intervalsOrdered = ScanQueryRunnerFactory.getIntervalsFromSpecificQuerySpec(query.getQuerySegmentSpec());
    if (query.getOrder().equals(ScanQuery.Order.DESCENDING)) {
      intervalsOrdered = Lists.reverse(intervalsOrdered);
    }
    // TODO: Create actual scans
    List<OperatorDefn> children = unknownList(query);
    if (query.getOrder() == ScanQuery.Order.NONE) {
      // Use normal strategy
      OperatorDefn concat = concat(children);
      if (query.isLimited()) {
        return new ScanResultLimitOperator.Defn(query, concat);
      }
      return concat;
    }
    int maxRowsQueuedForOrdering = (query.getMaxRowsQueuedForOrdering() == null
        ? scanQueryConfig.getMaxRowsQueuedForOrdering()
        : query.getMaxRowsQueuedForOrdering());
    if (query.getScanRowsLimit() <= maxRowsQueuedForOrdering) {
      // Use sort strategy
      // TODO: Group by interval as for the n-way merge
      return new ScanResultSortOperator.Defn(query, concat(children));
    }
    // Use n-way merge strategy using a priority queue
    List<Pair<Interval, OperatorDefn>> intervalsAndRunnersOrdered = new ArrayList<>();
    if (intervalsOrdered.size() == children.size()) {
      for (int i = 0; i < children.size(); i++) {
        intervalsAndRunnersOrdered.add(new Pair<>(intervalsOrdered.get(i), children.get(i)));
      }
      // TODO
//    } else if (queryRunners instanceof SinkQueryRunners) {
//      ((SinkQueryRunners<ScanResultValue>) queryRunners).runnerIntervalMappingIterator()
//                                                        .forEachRemaining(intervalsAndRunnersOrdered::add);
    } else {
      throw new ISE("Number of segment descriptors does not equal number of "
                    + "query runners...something went wrong!");
    }
    // Group the list of pairs by interval.  The LinkedHashMap will have an interval paired with a list of all the
    // operators for that segment
    LinkedHashMap<Interval, List<Pair<Interval, OperatorDefn>>> partitionsGroupedByInterval =
        intervalsAndRunnersOrdered.stream()
                                  .collect(Collectors.groupingBy(
                                      x -> x.lhs,
                                      LinkedHashMap::new,
                                      Collectors.toList()
                                  ));

    // Find the segment with the largest numbers of partitions.  This will be used to compare with the
    // maxSegmentPartitionsOrderedInMemory limit to determine if the query is at risk of consuming too much memory.
    int maxNumPartitionsInSegment =
        partitionsGroupedByInterval.values()
                                   .stream()
                                   .map(x -> x.size())
                                   .max(Comparator.comparing(Integer::valueOf))
                                   .get();
    int maxSegmentPartitionsOrderedInMemory = query.getMaxSegmentPartitionsOrderedInMemory() == null
        ? scanQueryConfig.getMaxSegmentPartitionsOrderedInMemory()
        : query.getMaxSegmentPartitionsOrderedInMemory();
    if (maxNumPartitionsInSegment > maxSegmentPartitionsOrderedInMemory) {
      throw ResourceLimitExceededException.withMessage(
          "Time ordering is not supported for a Scan query with %,d segments per time chunk and a row limit of %,d. "
          + "Try reducing your query limit below maxRowsQueuedForOrdering (currently %,d), or using compaction to "
          + "reduce the number of segments per time chunk, or raising maxSegmentPartitionsOrderedInMemory "
          + "(currently %,d) above the number of segments you have per time chunk.",
          maxNumPartitionsInSegment,
          query.getScanRowsLimit(),
          maxRowsQueuedForOrdering,
          maxSegmentPartitionsOrderedInMemory
      );
    }
    // Use n-way merge strategy

    // Create a list of grouped runner lists (i.e. each sublist/"runner group" corresponds to an interval) ->
    // there should be no interval overlap.  We create a list of lists so we can create a sequence of sequences.
    // There's no easy way to convert a LinkedHashMap to a sequence because it's non-iterable.
    List<List<OperatorDefn>> groupedRunners =
        partitionsGroupedByInterval.entrySet()
                                   .stream()
                                   .map(entry -> entry.getValue()
                                                      .stream()
                                                      .map(segQueryRunnerPair -> segQueryRunnerPair.rhs)
                                                      .collect(Collectors.toList()))
                                   .collect(Collectors.toList());

    // Starting from the innermost map:
    // (1) Disaggregate each ScanResultValue returned by the input operators
    // (2) Do a n-way merge per interval group based on timestamp
    // (3) Concatenate the groups
    OperatorDefn result = concat(groupedRunners
      .stream()
      .map(group -> new ScanResultMergeOperator.Defn(
          query,
          group
            .stream()
            .map(input -> new DisaggregateScanResultOperator.Defn(input))
            .collect(Collectors.toList())))
      .collect(Collectors.toList()));
    if (query.isLimited()) {
      return new ScanResultLimitOperator.Defn(query, result);
    }
    return result;
  }

  public OperatorDefn concat(List<OperatorDefn> children) {
    return ConcatOperator.concatOrNot(children);
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
