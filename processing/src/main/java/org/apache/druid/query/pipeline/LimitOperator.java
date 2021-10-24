package org.apache.druid.query.pipeline;

import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;

public abstract class LimitOperator implements Operator {

  public static class LimitDefn extends SingleChildDefn
  {
    public long limit = Long.MAX_VALUE;
  }

  protected final long limit;
  protected final Operator input;
  protected final ResponseContext responseContext;
  protected long rowCount;
  protected int batchCount;

  public LimitOperator(LimitDefn defn, Operator input, FragmentContext context)
  {
    this.limit = defn.limit;
    this.input = input;
    this.responseContext = context.responseContext();
  }

  @Override
  public void start()
  {
    input.start();
  }

  @Override
  public boolean hasNext() {
    return rowCount < limit && input.hasNext();
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      input.close(cascade);
    }
  }
}
