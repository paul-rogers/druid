package org.apache.druid.query.pipeline;

import java.util.Iterator;
import java.util.List;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanResultValue;

import com.google.common.base.Preconditions;

/**
 * Convert a batched set of scan result values to one-row "batches"
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#stableLimitingSort}
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#nWayMergeAndLimit}
 */
public class DisaggregateScanResultOperator implements Operator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.size() == 1);
      return new DisaggregateScanResultOperator(children.get(0));
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends SingleChildDefn
  {
    public Defn(OperatorDefn child) {
      this.child = child;
    }
  }

  private final Operator child;
  private Iterator<ScanResultValue> valueIter;

  public DisaggregateScanResultOperator(Operator child)
  {
    this.child = child;
  }

  @Override
  public void start()
  {
    child.start();
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (valueIter == null) {
        if (!child.hasNext()) {
          return false;
        }
        ScanResultValue value = (ScanResultValue) child.next();
        valueIter = value.toSingleEventScanResultValues().iterator();
      }
      if (valueIter.hasNext()) {
        return true;
      }
      valueIter = null;
    }
  }

  @Override
  public Object next() {
    return valueIter.next();
  }

  @Override
  public void close(boolean cascade) {
    if (cascade) {
      child.close(cascade);
    }
  }
}
