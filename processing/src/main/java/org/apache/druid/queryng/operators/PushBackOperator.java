package org.apache.druid.queryng.operators;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

/**
 * Operator which allows pushing a row back onto the input. The "pushed"
 * row can occur at construction time, or during execution.
 */
public class PushBackOperator<T> implements IterableOperator<T>
{
  private final Operator<T> input;
  private ResultIterator<T> inputIter;
  private T pushed;

  public PushBackOperator(
      FragmentContext context,
      Operator<T> input,
      ResultIterator<T> inputIter,
      T pushed
  )
  {
    this.input = input;
    this.inputIter = inputIter;
    this.pushed = pushed;
    context.register(this);
    context.registerChild(this, input);
    context.updateProfile(this, OperatorProfile.silentOperator(this));
  }

  public PushBackOperator(FragmentContext context, Operator<T> input)
  {
    this(context, input, null, null);
    context.updateProfile(this, OperatorProfile.silentOperator(this));
  }

  @Override
  public ResultIterator<T> open()
  {
    if (inputIter == null) {
      inputIter = input.open();
    }
    return this;
  }

  @Override
  public T next() throws EofException
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
