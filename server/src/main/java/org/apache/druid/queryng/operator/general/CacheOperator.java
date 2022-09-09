package org.apache.druid.queryng.operator.general;

import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;

public class CacheOperator<T> implements Operator<T>
{
  private final FragmentContext context;
  private final String cacheKey;

  @Override
  public ResultIterator<T> open()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close(boolean cascade)
  {
    // TODO Auto-generated method stub

  }
}
