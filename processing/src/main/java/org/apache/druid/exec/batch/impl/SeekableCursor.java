package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.operator.BatchReader.BatchCursor;

public class SeekableCursor implements BatchCursor
{
  public interface PositionListener
  {
    void updatePosition(int posn);
  }

  protected int size;
  protected PositionListener listener;
  protected int posn;

  public SeekableCursor()
  {
    this.listener = p -> { };
  }

  public void bind(int size)
  {
    this.size = size;
    reset();
  }

  public void bindListener(final PositionListener listener)
  {
    this.listener = listener;
  }

  @Override
  public void reset()
  {
    // If the batch is empty, start at EOF. Else, start
    // before the first row.
    this.posn = size == 0 ? 0 : -1;
  }

  @Override
  public boolean next()
  {
    if (++posn >= size) {
      posn = size();
      return false;
    }
    listener.updatePosition(posn);
    return true;
  }

  @Override
  public boolean seek(int newPosn)
  {
    // Bound the new position to the valid range. If the
    // batch is empty, the new position will be -1: before the
    // (non-existent) first row.
    if (newPosn < 0) {
      reset();
      return false;
    } else if (newPosn >= size()) {
      posn = size();
      return false;
    }
    posn = newPosn;
    listener.updatePosition(posn);
    return true;
  }

  @Override
  public int index()
  {
    return posn;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public boolean isEOF()
  {
    return posn == size;
  }
}
