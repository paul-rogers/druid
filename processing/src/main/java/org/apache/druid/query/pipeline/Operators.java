package org.apache.druid.query.pipeline;

import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;

import java.util.Iterator;

public class Operators
{

  public static Iterator<Object> toIterator(Operator op) {
    return new Iterator<Object>() {

      @Override
      public boolean hasNext()
      {
        return op.next();
      }

      @Override
      public Object next()
      {
        return op.get();
      }
    };
  }

  public static Iterable<Object> toIterable(Operator op) {
    return new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator()
      {
        return toIterator(op);
      }
    };
  }

  public static Sequence<Object> toSequence(Operator op) {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Object, Iterator<Object>>()
        {
          @Override
          public Iterator<Object> make()
          {
            return toIterator(op);
          }

          @Override
          public void cleanup(Iterator<Object> iterFromMake)
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
