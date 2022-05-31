package org.apache.druid.queryng.fragment;

import com.google.common.base.Preconditions;
import org.apache.druid.queryng.fragment.FragmentBuilder.ResultIterator;
import org.apache.druid.queryng.fragment.FragmentContext.State;
import org.apache.druid.queryng.operators.Operator;

import java.util.Iterator;

/**
 * Temporary way to run a fragment assembled in a distributed fashion across
 * a bunch of query runners. In this structure, there is no real "fragment",
 * we invent one out of thin air. This operator catches errors on every call,
 * updating the fragment context, and ensuring that the fragment context closes
 * all operators when this operator is closed.
 */
public class ResultIteratorImpl<T> implements ResultIterator<T>
{
  private final FragmentContextImpl context;
  private Iterator<T> rootIter;

  public ResultIteratorImpl(FragmentContextImpl context, Operator<T> root)
  {
    this.context = context;
    Preconditions.checkState(context.state == State.START);
    try {
      rootIter = root.open();
      context.state = State.RUN;
    }
    catch (Exception e) {
      context.failed(e);
      context.state = State.FAILED;
      throw e;
    }
  }

//  @Override
//  public Iterator<T> iterator()
//  {
//    return this;
//  }

  @Override
  public boolean hasNext()
  {
    Preconditions.checkState(context.state == State.RUN);
    try {
      return rootIter.hasNext();
    }
    catch (Exception e) {
      context.failed(e);
      context.state = State.FAILED;
      throw e;
    }
  }

  @Override
  public T next()
  {
    Preconditions.checkState(context.state == State.RUN);
    try {
      return rootIter.next();
    }
    catch (Exception e) {
      context.failed(e);
      context.state = State.FAILED;
      throw e;
    }
  }

  @Override
  public void close()
  {
    if (context.state == State.RUN || context.state == State.FAILED) {
      context.close();
    }
    context.state = State.CLOSED;
  }
}
