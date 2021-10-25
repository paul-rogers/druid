package org.apache.druid.query.pipeline;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.collections.StableLimitingSorter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.joda.time.Interval;

import com.google.common.base.Preconditions;

/**
 * Returns a sorted and limited transformation of the input. Materializes the full sequence
 * in memory before returning it. The amount of memory use is limited by the limit of the scan query.
 * <p>
 * Simultaneously sorts and limits its input.
 * <p>
 * The sort is stable, meaning that equal elements (as determined by the comparator) will not be reordered.
 * <p>
 * Not thread-safe.
 * <p>
 * The output is a set of batches each containing a single row.
 * <p>
 * Questions:<ul>
 * <li>Are the segments sorted by date? If so, why another sort? Why not a merge?</li>
 * <li>Segments are ordered by date. Segments within a data source do not overlay.
 * So, we can sort all the partitions for one segment and return them before reading
 * any of the partitions of the next segment. Doing so will reduce the memory footprint.</li>
 * </ul>
 *
 * @see {@link ScanQueryRunnerFactory#stableLimitingSort}
 * @see {@link org.apache.druid.collections.StableLimitingSorter}
 */
public class ScanResultSortOperator implements Operator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.size() == 1);
      return new ScanResultSortOperator((Defn) defn, children.get(0), context);
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends SingleChildDefn
  {
    public int limit;
    public Comparator<ScanResultValue> comparator;
    public List<Interval> intervalsOrdered;
    public ResultFormat resultFormat;

    // See ScanQueryRunnerFactory.stableLimitingSort
    public Defn(ScanQuery query, OperatorDefn child)
    {
      this.child = child;
      this.comparator = query.getResultOrdering();
      // Converting the limit from long to int could theoretically throw an ArithmeticException but this branch
      // only runs if limit < MAX_LIMIT_FOR_IN_MEMORY_TIME_ORDERING (which should be < Integer.MAX_VALUE)
      if (query.isLimited()) {
        if (query.getScanRowsLimit() > Integer.MAX_VALUE) {
          throw new UOE(
              "Limit of %,d rows not supported for priority queue strategy of time-ordering scan results",
              query.getScanRowsLimit()
          );
        }
        this.limit = Math.toIntExact(query.getScanRowsLimit());
      } else {
        this.limit = Integer.MAX_VALUE;
      }
      this.intervalsOrdered = ScanQueryRunnerFactory.getIntervalsFromSpecificQuerySpec(query.getQuerySegmentSpec());
      this.resultFormat = query.getResultFormat();
    }
  }

  private final Defn defn;
  private final Operator child;
  private Iterator<ScanResultValue> resultIter;

  public ScanResultSortOperator(Defn defn, Operator child,
      FragmentContext context) {
    this.defn = defn;
    this.child = child;
  }

  // See ScanQueryRunnerFactory.stableLimitingSort
  @Override
  public void start() {
    child.start();
    StableLimitingSorter<ScanResultValue> sorter = new StableLimitingSorter<>(defn.comparator, defn.limit);
    // We need to scan limit elements and anything else in the last segment
    int numRowsScanned = 0;
    for (Object input : Operators.toIterable(child)) {
      ScanResultValue next = (ScanResultValue) input;
      Interval finalInterval = null;
      // Using an intermediate unbatched ScanResultValue is not that great memory-wise, but the column list
      // needs to be preserved for queries using the compactedList result format
      for (ScanResultValue srv : next.toSingleEventScanResultValues()) {
        numRowsScanned++;
        sorter.add(srv);

        // Finish scanning the interval containing the limit row
        if (numRowsScanned > defn.limit && finalInterval == null) {
          long timestampOfLimitRow = srv.getFirstEventTimestamp(defn.resultFormat);
          for (Interval interval : defn.intervalsOrdered) {
            if (interval.contains(timestampOfLimitRow)) {
              finalInterval = interval;
            }
          }
          if (finalInterval == null) {
            throw new ISE("Row came from an unscanned interval");
          }
        }
      }
      // TODO: The format should be metadata in the batch
      if (finalInterval != null &&
          !finalInterval.contains(next.getFirstEventTimestamp(defn.resultFormat))) {
        break;
      }
    }
    resultIter = sorter.drain();
    child.close(true);
  }

  @Override
  public boolean hasNext() {
    return resultIter.hasNext();
  }

  @Override
  public Object next() {
    return resultIter.next();
  }

  @Override
  public void close(boolean cascade)
  {
  }
}
