package org.apache.druid.query.pipeline;

import java.util.Iterator;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;

import com.google.common.base.Preconditions;

/**
 * Iterator over a sequence.
 */
public class SequenceIterator<T> implements Iterator<T>
{
  private Yielder<T> yielder;

  public static <T> SequenceIterator<T> of(Sequence<T> sequence)
  {
    return new SequenceIterator<T>(sequence);
  }

  public SequenceIterator(Sequence<T> sequence) {
     this.yielder = sequence.toYielder(null,
         new YieldingAccumulator<T, T>()
         {
            @Override
            public T accumulate(T accumulated, T in) {
              yield();
              return in;
            }
         }
    );
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public T next() {
    Preconditions.checkState(!yielder.isDone());
    T value = yielder.get();
    yielder = yielder.next(null);
    return value;
  }
}
