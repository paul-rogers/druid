package org.apache.druid.exec.internalSort;

import com.google.common.base.Stopwatch;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.Operator.IterableOperator;
import org.apache.druid.exec.operator.OperatorProfile;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.AbstractUnaryOperator;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.frame.key.SortColumn;

import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class InternalSortOperator extends AbstractUnaryOperator implements IterableOperator
{
  protected final List<SortColumn> keys;
  private ResultIterator resultIter;
  protected int rowCount;
  protected int batchCount;
  protected long sortTimeMs;

  public InternalSortOperator(FragmentContext context, InternalSortOp plan, List<Operator> children)
  {
    super(context, children);
    this.keys = plan.keys();
  }

  @Override
  public ResultIterator open()
  {
    openInput();
    resultIter = () -> sort();
    return this;
  }

  private Batch sort() throws StallException
  {
    Stopwatch stopwatch = Stopwatch.createStarted();
    resultIter = doSort();
    sortTimeMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    return resultIter.next();
  }

  protected abstract ResultIterator doSort() throws StallException;

  @Override
  public Batch next() throws StallException
  {
    return resultIter.next();
  }

  @Override
  public void close(boolean cascade)
  {
    closeInput();
    resultIter = null;
    OperatorProfile profile = new OperatorProfile("Internal Sort");
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    profile.add(OperatorProfile.BATCH_COUNT_METRIC, batchCount);
    profile.add("sortTimeMs", sortTimeMs);
    context.updateProfile(this, profile);
  }
}
