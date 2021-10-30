package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * This operator iterates over a ScanResultValue operator.  Its behaviour
 * varies depending on whether the query is returning time-ordered values and whether the CTX_KEY_OUTERMOST
 * flag is false.
 * <p>
 * Behaviours:
 * <ol>
 * <li>No time ordering: expects the child to produce ScanResultValues which each contain up to query.batchSize events.
 *     The operator will be "done" when the limit of events is reached.  The final ScanResultValue might contain
 *     fewer than batchSize events so that the limit number of events is returned.</li>
 * <li>Time Ordering, CTX_KEY_OUTERMOST false: Same behaviour as no time ordering.</li>
 * <li>Time Ordering, CTX_KEY_OUTERMOST=true or null: The child operator in this case should produce ScanResultValues
 *    that contain only one event each for the CachingClusteredClient n-way merge.  This operator will perform
 *    batching according to query batch size until the limit is reached.</li>
 * </ol>
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator}
 */
public class ScanResultLimitOperator extends LimitOperator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.size() == 1);
      return new ScanResultLimitOperator((Defn) defn, children.get(0), context);
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends LimitOperator.LimitDefn
  {
    public boolean grouped;
    public int batchSize;

    @VisibleForTesting
    public Defn()
    {
    }

    public Defn(ScanQuery query, OperatorDefn child) {
      this.child = child;
      ScanQuery.ResultFormat resultFormat = query.getResultFormat();
      if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
        throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
      }
      this.limit = query.getScanRowsLimit();
      this.grouped = query.getOrder() == ScanQuery.Order.NONE ||
          !query.getContextBoolean(ScanQuery.CTX_KEY_OUTERMOST, true);
      this.batchSize = query.getBatchSize();
    }
  }

  private final Defn defn;

  public ScanResultLimitOperator(Defn defn, Operator input, FragmentContext context)
  {
    super(defn, input, context);
    this.defn = defn;
  }

  @Override
  public Object next() {
    batchCount++;
    if (defn.grouped) {
      return groupedNext();
    } else {
      return ungroupedNext();
    }
  }

  private ScanResultValue groupedNext() {
    ScanResultValue batch = (ScanResultValue) inputIter.next();
    List<?> events = (List<?>) batch.getEvents();
    if (events.size() <= limit - rowCount) {
      rowCount += events.size();
      return batch;
    } else {
      // last batch
      // single batch length is <= rowCount.MAX_VALUE, so this should not overflow
      int numLeft = (int) (limit - rowCount);
      rowCount = limit;
      return new ScanResultValue(batch.getSegmentId(), batch.getColumns(), events.subList(0, numLeft));
    }
  }

  private Object ungroupedNext() {
    // Perform single-event ScanResultValue batching at the outer level.  Each scan result value from the yielder
    // in this case will only have one event so there's no need to iterate through events.
    List<Object> eventsToAdd = new ArrayList<>(defn.batchSize);
    List<String> columns = new ArrayList<>();
    while (eventsToAdd.size() < defn.batchSize && inputIter.hasNext() && rowCount < limit) {
      ScanResultValue srv = (ScanResultValue) inputIter.next();
      // Only replace once using the columns from the first event
      columns = columns.isEmpty() ? srv.getColumns() : columns;
      eventsToAdd.add(Iterables.getOnlyElement((List<?>) srv.getEvents()));
      rowCount++;
    }
    return new ScanResultValue(null, columns, eventsToAdd);
  }
}
