package org.apache.druid.exec.window;

import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.impl.AbstractUnaryOperator;
import org.apache.druid.exec.plan.WindowSpec;

public class WindowOperator extends AbstractUnaryOperator
{

  public WindowOperator(FragmentContext context, WindowSpec spec, Operator input)
  {
    super(context, input);
  }

}
