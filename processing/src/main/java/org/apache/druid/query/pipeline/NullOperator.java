package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.Iterator;

import com.google.common.base.Preconditions;

/**
 * World's simplest operator: does absolutely nothing
 * (other than check that the protocol is followed.) Used in
 * tests when we want an empty input.
 */
public class NullOperator implements Operator
{
  public State state = State.START;

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    Preconditions.checkState(state == State.START);
    state = State.RUN;
    return Collections.emptyIterator();
  }

  @Override
  public void close(boolean cascade) {
    state = State.CLOSED;
  }
}
