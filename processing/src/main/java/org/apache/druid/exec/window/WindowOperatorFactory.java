package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorFactory;
import org.apache.druid.exec.plan.OperatorSpec;
import org.apache.druid.exec.plan.WindowSpec;

import java.util.List;

public class WindowOperatorFactory implements OperatorFactory
{

  @Override
  public Class<? extends OperatorSpec> accepts()
  {
    return WindowSpec.class;
  }

  @Override
  public Operator create(FragmentContext context, OperatorSpec spec, List<Operator> children)
  {
    Preconditions.checkArgument(children.size() == 1);
    return new WindowOperator(context, (WindowSpec) spec, children.get(0));
  }
}
