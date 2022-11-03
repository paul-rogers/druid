package org.apache.druid.exec.batch;

import org.apache.druid.java.util.common.UOE;

/**
 * Positioner for a batch of rows. Most typically combined with a {@link BatchReader}
 * to provide a {@link BatchCursor}, but may also be used independently for specialized
 * batch iteration.
 * <p>
 * There are two ways to work with a batch: sequentially
 * (via the base interface {@link RowSequencer} methods), or or randomly as defined
 * here. This class adds the ability to make multiple sequential passes over a
 * batch: call {@link #reset()} to start another pass.
 * <p>
 * To read randomly, call {@link #size()} to determine the row count, then call
 * {@link #seek(int)} to move to a specific row.
 * <p>
 * The positioner itself is driven via two listeners. First, the positioner is
 * a {@link BindingListener} from which it learns about a reader being bound to a
 * new batch. That event provides the size of the batch (which may be zero), and
 * causes the positioner to reset to before the first row of the batch (or at EOF,
 * if the batch is empty.)
 * <p>
 * Second, the positioner takes a {@link PositionListener} to which it announces
 * each time the current index moves. The reader uses this event to position itself
 * to read that row. A row of -1 means that the positioner is placed on a non-existent
 * row (before first, after last, or implementation-specific beginning or end of a
 * block of rows.)
 */
public interface BatchPositioner extends RowSequencer, BindingListener
{
  void bindListener(PositionListener listener);

  /**
   * Position the reader before the first row of the batch, so that
   * the next call to {@link #next()} moves to the first row.
   *
   * @throws UOE if this is a one-pass cursor and cannot be reset.
   * Usually known from context. See the batch capabilities for a
   * dynamic indication.
   */
  void reset();

  int index();

  int size();

  /**
   * Seek to the given position. If the position is -1, the behavior is the
   * same as {@link #reset()}: position before the first row. If the position is
   * {@link #size()}, then the position is after the last row. Else, the
   * batch is positioned at the requested location.
   * @return {@code true} if the requested position is readable, {@code false}
   * if the position is before the start or after the end.
   */
  boolean seek(int posn);
}
