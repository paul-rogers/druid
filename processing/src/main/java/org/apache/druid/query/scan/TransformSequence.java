package org.apache.druid.query.scan;

import java.io.IOException;

import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;

public abstract class TransformSequence<T> implements Sequence<T>
{
  public interface InputCreator<T>
  {
    Sequence<T> open();
  }

  protected abstract void open();
  protected abstract void close();
  protected abstract boolean hasNext();
  protected abstract T next();

  @Override
  public <OutType> OutType accumulate(final OutType initValue, final Accumulator<OutType, T> fn)
  {
    open();
    OutType accumulated = initValue;

    try {
      while (hasNext()) {
        accumulated = fn.accumulate(accumulated, next());
      }
    }
    catch (Throwable t) {
      try {
        close();
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
    close();
    return accumulated;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue,
      YieldingAccumulator<OutType, T> accumulator) {
    open();
    return (Yielder<OutType>) new TransformYielder();
  }

  private class TransformYielder implements Yielder<T>
  {
    private T value;

    public TransformYielder() {
      next(null);
    }

    @Override
    public boolean isDone() {
      return value == null;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public Yielder<T> next(T initValue) {
      if (hasNext()) {
        value = TransformSequence.this.next();
      } else {
        value = null;
      }
      return this;
    }

    @Override
    public void close() throws IOException {
      TransformSequence.this.close();
    }
  }
}
