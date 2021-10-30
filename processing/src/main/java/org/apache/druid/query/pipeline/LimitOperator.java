package org.apache.druid.query.pipeline;

import java.util.Iterator;

import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.Operator.IterableOperator;

public abstract class LimitOperator implements IterableOperator {

  public static class LimitDefn extends SingleChildDefn
  {
    public long limit = Long.MAX_VALUE;
  }

  protected final long limit;
  protected final Operator input;
  protected final ResponseContext responseContext;
  protected Iterator<Object> inputIter;
  protected long rowCount;
  protected int batchCount;

  public LimitOperator(LimitDefn defn, Operator input, FragmentContext context)
  {
    this.limit = defn.limit;
    this.input = input;
    this.responseContext = context.responseContext();
  }

  @Override
  public Iterator<Object> open()
  {
    inputIter = input.open();
    return this;
  }

  @Override
  public boolean hasNext() {
    return rowCount < limit && inputIter.hasNext();
  }

  @Override
  public Object next()
  {
    rowCount++;
    return inputIter.next();
  }

  @Override
  public void close(boolean cascade)
  {
    inputIter = null;
    if (cascade) {
      input.close(cascade);
    }
  }
}
