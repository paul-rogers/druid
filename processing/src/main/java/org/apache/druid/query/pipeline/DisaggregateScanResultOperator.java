package org.apache.druid.query.pipeline;

import java.util.Iterator;

import org.apache.druid.query.pipeline.Operator.IterableOperator;
import org.apache.druid.query.scan.ScanResultValue;

/**
 * Convert a batched set of scan result values to one-row "batches"
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#stableLimitingSort}
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#nWayMergeAndLimit}
 */
public class DisaggregateScanResultOperator implements IterableOperator
{
  private final Operator child;
  private Iterator<Object> childIter;
  private Iterator<ScanResultValue> valueIter;

  public DisaggregateScanResultOperator(Operator child)
  {
    this.child = child;
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    childIter = child.open(context);
    return this;
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (valueIter == null) {
        if (!childIter.hasNext()) {
          return false;
        }
        ScanResultValue value = (ScanResultValue) childIter.next();
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
    childIter = null;
  }
}
