package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Operator;

public abstract class AbstractOperator implements Operator
{
  protected final FragmentContext context;

  public AbstractOperator(final FragmentContext context)
  {
    this.context = context;
  }
}
