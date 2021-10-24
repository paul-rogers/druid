package org.apache.druid.query.pipeline;

import java.util.List;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Offset that skips a given number of rows on top of a skips ScanQuery. It is used to implement
 * the "offset" feature.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryOffsetSequence}
 */
public class ScanResultOffsetOperator implements Operator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.size() == 1);
      return new ScanResultOffsetOperator((Defn) defn, children.get(0), context);
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends SingleChildDefn
  {
    public long offset;

    @VisibleForTesting
    public Defn()
    {
    }

    public Defn(ScanQuery query) {
      ScanQuery.ResultFormat resultFormat = query.getResultFormat();
      if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
        throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
      }
      this.offset = query.getScanRowsOffset();
      if (offset < 1) {
        throw new IAE("'offset' must be greater than zero");
      }
    }
  }

  private final Operator input;
  private final Defn defn;
  private long rowCount;
  @SuppressWarnings("unused")
  private int batchCount;
  private ScanResultValue lookAhead;
  private boolean done;

  private ScanResultOffsetOperator(Defn defn, Operator input, FragmentContext context)
  {
    this.defn = defn;
    this.input = input;
  }

  @Override
  public void start()
  {
    input.start();
  }

  @Override
  public boolean hasNext()
  {
    if (done) {
      return false;
    }
    if (rowCount == 0) {
      return skip();
    }
    done = !input.hasNext();
    return !done;
  }

  @Override
  public Object next()
  {
    if (lookAhead != null) {
      ScanResultValue result = lookAhead;
      lookAhead = null;
      return result;
    }
    return input.next();
  }

  private boolean skip()
  {
    while (true) {
      if (!input.hasNext()) {
        done = true;
        return false;
      }
      ScanResultValue batch = (ScanResultValue) input.next();
      final List<?> rows = (List<?>) batch.getEvents();
      final int eventCount = rows.size();
      final long toSkip = defn.offset - rowCount;
      if (toSkip >= eventCount) {
        rowCount += eventCount;
        continue;
      }
      rowCount += eventCount - toSkip;
      lookAhead = new ScanResultValue(
          batch.getSegmentId(),
          batch.getColumns(),
          rows.subList((int) toSkip, eventCount));
      return true;
    }
  }

//  private boolean ungroupedSkip() {
//    while (rowCount < defn.offset) {
//      if (!input.hasNext()) {
//        done = true;
//        return false;
//      }
//      input.next();
//      rowCount++;
//    }
//    return input.hasNext();
//  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      input.close(cascade);
    }
  }
}
