package org.apache.druid.query.pipeline;

import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;

import java.util.Iterator;

public class Operators
{

  public static Iterable<Object> toIterable(Operator op) {
    return new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator()
      {
        return op;
      }
    };
  }

  public static <T> Sequence<T> toSequence(Operator op) {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @SuppressWarnings("unchecked")
          @Override
          public Iterator<T> make()
          {
            return (Iterator<T>) op;
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            // No cleanup: fragment runner will close operators
          }
        }
    );
  }

  public static Operator toOperator(Sequence<?> sequence)
  {
    return new SequenceOperator(sequence);
  }
}
