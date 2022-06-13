package org.apache.druid.queryng.operators;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.Iterator;

/**
 * Operator which allows pushing a row back onto the input. The "pushed"
 * row can occur at construction time, or during execution.
 */
public class PushBackOperator<T> implements IterableOperator<T>
{
  private final Operator<T> input;
  private Iterator<T> inputIter;
  private T pushed;

  public PushBackOperator(
      FragmentContext context,
      Operator<T> input,
      Iterator<T> inputIter,
      T pushed)
  {
    this.input = input;
    this.inputIter = inputIter;
    this.pushed = pushed;
    context.register(this);
  }

  public PushBackOperator(FragmentContext context, Operator<T> input)
  {
    this(context, input, null, null);
  }

  @Override
  public Iterator<T> open()
  {
    if (inputIter == null) {
      inputIter = input.open();
    }
    return this;
  }

  @Override
  public boolean hasNext()
  {
    return pushed != null || inputIter != null && inputIter.hasNext();
  }

  @Override
  public T next()
  {
    if (pushed != null) {
      T ret = pushed;
      pushed = null;
      return ret;
    }
    return inputIter.next();
  }

  public void push(T item)
  {
    if (pushed != null) {
      throw new ISE("Cannot push more than one items onto PushBackOperator");
    }
    pushed = item;
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      input.close(cascade);
    }
    inputIter = null;
    pushed = null;
  }
}
