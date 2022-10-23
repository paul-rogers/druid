package org.apache.druid.exec.internalSort;

import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorFactory;
import org.apache.druid.exec.operator.OperatorSpec;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.java.util.common.UOE;

import java.util.List;

public class InternalSortFactory implements OperatorFactory
{
  @Override
  public Operator create(FragmentContext context, OperatorSpec plan, List<Operator> children)
  {
    InternalSortOp sortOp = (InternalSortOp) plan;
    switch (sortOp.sortType()) {
      case ROW:
        return new RowInternalSortOperator(context, sortOp, children);
      default:
        throw new UOE(sortOp.sortType().name());
    }
  }
}
