package org.apache.druid.query.pipeline;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;

import java.io.IOException;

/**
 * The <code>SequenceOperator</code> wraps a {@link Sequence} in the
 * operator protocol. The operator will make (at most) one pass through
 * the sequence. The sequence's yielder will be defined in <code>start()</code>,
 * which may cause the sequence to start doing work and obtaining resources.
 * Each call to <code>next()</code>/<code>get()</code> will yield one result
 * from the sequence. The <code>close()</code> call will close the yielder
 * for the sequence, which should release any resources held by the sequence.
 *
 * @param <T>
 */
public class SequenceOperator<T> implements Operator<T>
{
  private enum State
  {
    NEW, START, ACTIVE, DONE, CLOSED
  }
  private final Sequence<T> sequence;
  private Yielder<T> yielder;
  private State state = State.NEW;

  public SequenceOperator(Sequence<T> sequence)
  {
    this.sequence = sequence;
  }

  @Override
  public void start()
  {
    Preconditions.checkState(state == State.NEW);
    yielder = sequence.toYielder(
        null,
        new YieldingAccumulator<T, T>()
        {
           @Override
          public T accumulate(T accumulated, T in)
          {
            yield();
            return in;
          }
        }
    );
    state = yielder == null ? State.DONE : State.START;
  }

  @Override
  public boolean next()
  {
    switch (state) {
    case START:
      state = State.ACTIVE;
      return true;
    case ACTIVE:
      break;
    default:
       return false;
    }
    yielder = yielder.next(null);
    if (yielder.isDone()) {
      state = State.DONE;
      return false;
    }
    return true;
  }

  @Override
  public T get()
  {
    Preconditions.checkState(yielder != null);
    Preconditions.checkState(state == State.ACTIVE);
    return yielder.get();
  }

  @Override
  public void close()
  {
    state = State.CLOSED;
    if (yielder == null) {
      return;
    }
    try {
      yielder.close();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    finally {
      yielder = null;
    }
  }
}
