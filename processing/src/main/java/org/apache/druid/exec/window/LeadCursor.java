package org.apache.druid.exec.window;

import org.apache.druid.exec.window.BufferCursor.Listener;
import org.apache.druid.java.util.common.ISE;

import java.util.List;

public class LeadCursor
{
  private class LagListener implements Listener
  {
    @Override
    public void exitBatch(int batchIndex)
    {
      buffer.unloadBatch(batchIndex);
    }

    @Override
    public boolean requestBatch(int batchIndex)
    {
      throw new ISE("Method should not be called");
    }
  }

  private class LeadListener implements Listener
  {
    @Override
    public void exitBatch(int batchIndex)
    {
    }

    @Override
    public boolean requestBatch(int batchIndex)
    {
      return buffer.loadBatch(batchIndex);
    }
  }

  private class NoLeadOrLagListener implements Listener
  {
    @Override
    public void exitBatch(int batchIndex)
    {
      buffer.unloadBatch(batchIndex);
    }

    @Override
    public boolean requestBatch(int batchIndex)
    {
      return buffer.loadBatch(batchIndex);
    }
  }

  private final BatchBuffer2 buffer;
  private final BufferCursor primaryCursor;
  private final List<BufferCursor> followerCursors;

  public LeadCursor(BatchBuffer2 buffer, BufferCursor primaryCursor, List<BufferCursor> followerCursors)
  {
    this.buffer = buffer;
    this.primaryCursor = primaryCursor;
    this.followerCursors = followerCursors;
    BufferCursor lead = null;
    BufferCursor lag = null;
    for (BufferCursor cursor : followerCursors) {
      int offset = cursor.offset();
      if (offset < 0) {
        if (lag == null || offset < lag.offset()) {
          lag = cursor;
        }
      } else if (offset > 0) {
        if (lead == null || offset > lead.offset()) {
          lead = cursor;
        }
      }
    }
    if (lead != null) {
      lead.bindListener(new LeadListener());
      if (lag == null) {
        primaryCursor.bindListener(new LagListener());
      }
    }
    if (lag != null) {
      lag.bindListener(new LagListener());
      if (lead == null) {
        primaryCursor.bindListener(new LeadListener());
      }
    }
    if (lead == null && lag == null) {
      primaryCursor.bindListener(new NoLeadOrLagListener());
    }
  }

  public boolean next()
  {
    if (!primaryCursor.next()) {
      return false;
    }
    for (BufferCursor cursor : followerCursors) {
      cursor.next();
    }
    return true;
  }
}
