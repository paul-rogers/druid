package org.apache.druid.query.pipeline;

import java.util.Iterator;

import org.apache.druid.query.pipeline.Operator.IterableOperator;

public abstract class LimitOperator implements IterableOperator
{
  public static final long UNLIMITED = Long.MAX_VALUE;

  protected final long limit;
  protected final Operator input;
  protected Iterator<Object> inputIter;
  protected long rowCount;
  protected int batchCount;

  public LimitOperator(long limit, Operator input)
  {
    this.limit = limit;
    this.input = input;
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    inputIter = input.open(context);
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
