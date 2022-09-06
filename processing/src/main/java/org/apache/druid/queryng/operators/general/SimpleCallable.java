package org.apache.druid.queryng.operators.general;

import org.apache.druid.query.AbstractPrioritizedQueryRunnerCallable;
import org.apache.druid.query.QueryRunner;

public abstract class SimpleCallable<V> extends AbstractPrioritizedQueryRunnerCallable<Object, V>
{
  public SimpleCallable(int priority, QueryRunner<V> runner)
  {
    super(priority, null);
  }
}
