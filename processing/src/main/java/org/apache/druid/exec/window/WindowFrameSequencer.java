package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.RowCursor.RowSequencer;
import org.apache.druid.exec.window.WindowFrameCursor.Listener;

import java.util.ArrayList;
import java.util.List;

public class WindowFrameSequencer implements RowSequencer
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
      // Only called if the lag cursor advances beyond the
      // last batch, which only occurs at EOF.
      return false;
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

  private final BatchBuffer buffer;
  private final WindowFrameCursor primaryCursor;
  private final List<WindowFrameCursor> cursors;

  public WindowFrameSequencer(BatchBuffer buffer, WindowFrameCursor primaryCursor, List<WindowFrameCursor> followerCursors)
  {
    this.buffer = buffer;
    this.primaryCursor = primaryCursor;
    WindowFrameCursor lead = null;
    WindowFrameCursor lag = null;
    for (WindowFrameCursor cursor : followerCursors) {
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

    // If a lead exists, then the largest lead will load batches.
    if (lead != null) {
      lead.bindListener(new LeadListener());

      // If no lag exists, the primary cursor releases batches.
      if (lag == null) {
        primaryCursor.bindListener(new LagListener());
      }
    }

    // If a lag exists, then the largest lag will release batches.
    if (lag != null) {
      lag.bindListener(new LagListener());

      // If no lead exists, the primary cursor loads batches.
      if (lead == null) {
        primaryCursor.bindListener(new LeadListener());
      }
    }

    // Rather boring: no lag or lead: the primary cursor does all the work.
    if (lead == null && lag == null) {
      primaryCursor.bindListener(new NoLeadOrLagListener());
    }

    this.cursors = new ArrayList<>();
    if (lead == null) {
      this.cursors.add(primaryCursor);
      this.cursors.addAll(followerCursors);
    } else {
      this.cursors.add(lead);
      this.cursors.add(primaryCursor);
      for (WindowFrameCursor cursor : followerCursors) {
        if (cursor != lead) {
          this.cursors.add(cursor);
        }
      }
    }
  }

  @Override
  public boolean next()
  {
    for (WindowFrameCursor cursor : cursors) {
      cursor.next();
    }
    return !primaryCursor.isEOF();
  }

  @Override
  public boolean isEOF()
  {
    return primaryCursor.isEOF();
  }

  @Override
  public boolean isValid()
  {
    return primaryCursor.isValid();
  }
}
