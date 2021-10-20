package org.apache.druid.query.pipeline;

import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;

import java.util.Iterator;

public class Operators
{

  public static <T> Iterator<T> toIterator(Operator<T> op) {
    return new Iterator<T>() {

      @Override
      public boolean hasNext()
      {
        return op.next();
      }

      @Override
      public T next()
      {
        return op.get();
      }
    };
  }

  public static <T> Iterable<T> toIterable(Operator<T> op) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator()
      {
        return toIterator(op);
      }
    };
  }

  public static <T> Sequence<T> toSequence(Operator<T> op) {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return toIterator(op);
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            // No cleanup: fragment runner will close operators
          }
        }
    );
  }

  public static <T> Operator<T> toOperator(Sequence<T> sequence)
  {
    return new SequenceOperator<T>(sequence);
  }
}
