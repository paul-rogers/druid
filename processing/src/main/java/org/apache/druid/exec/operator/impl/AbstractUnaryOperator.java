package org.apache.druid.exec.operator.impl;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.ResultIterator;

import java.util.List;

public abstract class AbstractUnaryOperator extends AbstractOperator
{
  protected final Operator input;
  protected ResultIterator inputIter;

  public AbstractUnaryOperator(FragmentContext context, List<Operator> children)
  {
    super(context);
    Preconditions.checkArgument(children.size() == 1);
    this.input = children.get(0);
  }

  public void openInput()
  {
    inputIter = input.open();
  }

  public void closeInput()
  {
    if (inputIter != null) {
      input.close(true);
    }
    inputIter = null;
  }
}
