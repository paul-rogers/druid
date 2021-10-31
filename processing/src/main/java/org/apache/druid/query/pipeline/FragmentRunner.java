package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.Operator.FragmentContextImpl;

import com.google.common.base.Preconditions;

public class FragmentRunner
{
  private final List<Operator> operators = new ArrayList<>();

  public Operator add(Operator op)
  {
    operators.add(op);
    return op;
  }

  public Operator root()
  {
    Preconditions.checkState(!operators.isEmpty());
    return operators.get(operators.size() - 1);
  }

  public <T> QueryRunner<T> toRunner()
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext) {
        return new BaseSequence<>(
            new BaseSequence.IteratorMaker<T, Iterator<T>>()
            {
              @SuppressWarnings("unchecked")
              @Override
              public Iterator<T> make()
              {
                return (Iterator<T>) root().open(
                    new FragmentContextImpl(
                        queryPlus.getQuery().getId(),
                        responseContext
                        )
                    );
              }

              @Override
              public void cleanup(Iterator<T> iterFromMake)
              {
                close();
              }
            }
        );
      }
    };
  }

  /**
   * Closes all operators from the leaves to the root.
   * As a result, operators must not call their children during
   * the {@code close()} call. Errors are collected, but all operators are closed
   * regardless of exceptions.
   */
  public void close()
  {
    List<Exception> exceptions = new ArrayList<>();
    for (Operator op : operators) {
      try {
        op.close(false);
      }
      catch (Exception e) {
        exceptions.add(e);
      }
    }
    // TODO: Do something with the exceptions
  }
}
