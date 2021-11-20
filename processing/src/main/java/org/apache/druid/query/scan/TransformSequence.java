package org.apache.druid.query.scan;

import java.io.Closeable;

import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
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

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue,
      YieldingAccumulator<OutType, T> accumulator) {
    open();
    try {
      return makeYielder(initValue, accumulator);
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
  }

  private <OutType> Yielder<OutType> makeYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    OutType retVal = initValue;
    while (!accumulator.yielded() && hasNext()) {
      retVal = accumulator.accumulate(retVal, next());
    }

    if (!accumulator.yielded()) {
      return Yielders.done(
          retVal,
          (Closeable) () -> TransformSequence.this.close()
      );
    }

    final OutType finalRetVal = retVal;
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return finalRetVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accumulator.reset();
        try {
          return makeYielder(initValue, accumulator);
        }
        catch (Throwable t) {
          try {
            TransformSequence.this.close();
          }
          catch (Exception e) {
            t.addSuppressed(e);
          }
          throw t;
        }
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close()
      {
        TransformSequence.this.close();
      }
    };
  }
}
