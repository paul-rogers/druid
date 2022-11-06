package org.apache.druid.exec.window.old;

import com.google.common.collect.Iterables;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.RowSequencer;
import org.apache.druid.exec.window.BatchBuffer;
import org.apache.druid.exec.window.old.Exp.PartitionBuilder.PartitionSequencer;
import org.apache.druid.exec.window.old.WindowFrameCursor.BatchEventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class WindowFrameSequencer implements RowSequencer
{
  private class LagListener implements BatchEventListener
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

  private class LeadListener implements BatchEventListener
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

  private class NoLeadOrLagListener implements BatchEventListener
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
  private final PartitionSequencer primaryCursor;
  private final List<PartitionSequencer> cursors;

  public WindowFrameSequencer(BatchBuffer buffer, PartitionSequencer primaryCursor, Map<Integer, BatchReader> followerCursors)
  {
    this.buffer = buffer;
    this.primaryCursor = primaryCursor;
    PartitionSequencer lead = null;
    int leadOffset = 0;
    PartitionSequencer lag = null;
    int lagOffset = 0;
    for (Entry<Integer, BatchReader> entry : followerCursors.entrySet()) {
      int offset = entry.getKey();
      BatchReader reader = entry.getValue();
      PartitionSequencer sequencer;
      if (offset == 0) {
        sequencer = new PrimarySequencer(reader);
      } else if (offset < 0) {
        sequencer = new LagSequencer(reader, -offset);
        if (lag == null || offset < lagOffset) {
          lag = sequencer;
          lagOffset = offset;
        }
      } else { // offset > 0
        sequencer = new LeadSequencer(reader, -offset);
        if (lead == null || offset > leadOffset) {
          lead = sequencer;
          leadOffset = offset;
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
      Iterables.addAll(this.cursors, followerCursors);
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

  public List<WindowFrameCursor> cursors()
  {
    return cursors;
  }

  public void startPartition()
  {
    for (WindowFrameCursor cursor : cursors) {
      cursor.startPartition();
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
