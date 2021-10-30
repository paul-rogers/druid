package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;

import com.google.common.base.Preconditions;

/**
 * World's simplest operator: does absolutely nothing
 * (other than check that the protocol is followed.) Used in
 * tests when we want an empty input.
 */
public class NullOperator implements Operator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.isEmpty());
      return new NullOperator();
    }
  };
  public static final Defn DEFN = new Defn();

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends LeafDefn
  {
  }

  public State state = State.START;

  @Override
  public Iterator<Object> open()
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
