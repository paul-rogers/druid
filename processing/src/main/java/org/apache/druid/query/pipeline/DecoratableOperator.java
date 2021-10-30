package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.List;
import org.apache.druid.query.pipeline.Operator.Decoratable;

public abstract class DecoratableOperator implements Operator, Decoratable
{
  public static class PassThroughOperator extends DecoratableOperator
  {
    private final Operator child;

    public PassThroughOperator(Operator child)
    {
      this.child = child;
    }

    @Override
    public void start() {
      super.start();
      child.start();
    }

    @Override
    public boolean hasNext() {
      return child.hasNext();
    }

    @Override
    public Object next() {
      return child.next();
    }

    @Override
    public void close(boolean cascade) {
      if (cascade) {
        child.close(cascade);
      }
      super.close(cascade);
    }
  }

  /**
   * Decorates an operator. If the operator is decoratable, then adds
   * the decorator. Otherwise, adds a "dummy" operator to hold the
   * decorator. Returns the possibly new operator to use to construct
   * the operator chain.
   */
  public static Operator decorate(Operator op, Decorator listener) {
    if (op instanceof Decoratable) {
      ((Decoratable) op).decorate(listener);
      return op;
    }
    PassThroughOperator newOp = new PassThroughOperator(op);
    newOp.decorate(listener);
    return newOp;
  }

  private enum State
  {
    NEW, RUN, CLOSED,
  }
  private List<Decorator> listeners;
  private State state = State.NEW;

  @Override
  public void decorate(Decorator listener)
  {
    if (listeners == null) {
      listeners = new ArrayList<>();
    }
    listeners.add(listener);
  }

  @Override
  public void start() {
    if (state != State.NEW) {
      return;
    }
    state = State.RUN;
    if (listeners == null) {
      return;
    }
    for (Decorator listener : listeners) {
      listener.before();
    }
  }

  @Override
  public void close(boolean cascade) {
    if (state != State.RUN || listeners == null) {
      state = State.CLOSED;
      return;
    }
    state = State.CLOSED;
    for (Decorator listener : listeners) {
      listener.before();
    }
  }

}
