package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchPositioner;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.RowCursor;
import org.apache.druid.exec.window.Partitioner.PartitionBounds;

public abstract class WindowFrameCursor implements RowCursor
{
  public interface BatchEventListener
  {
    void exitBatch(int batchIndex);
    boolean requestBatch(int batchIndex);
  }

  private static final BatchEventListener NO_OP_LISTENER = new BatchEventListener()
  {
    @Override
    public void exitBatch(int batchIndex)
    {
    }

    @Override
    public boolean requestBatch(int batchIndex)
    {
      return false;
    }
  };

  private enum State
  {
    BEFORE_FIRST,
    WITHIN_PARITION,
    AFTER_LAST;
  }

  protected final BatchBuffer buffer;
  protected final BatchCursor cursor;
  protected final BatchPositioner positioner;
  private BatchEventListener batchListener = NO_OP_LISTENER;
  protected PartitionBounds partitionBounds;

  // Start positioned before the first batch so that the first fetch
  // moves to the first row of the first batch (or EOF in the limit case)
  protected int batchIndex = -1;
  protected State state = State.BEFORE_FIRST;
  protected boolean eof;

  public WindowFrameCursor(BatchBuffer buffer)
  {
    this.buffer = buffer;
    this.cursor = Batches.toCursor(buffer.inputSchema.newReader());
    this.positioner = cursor.positioner();
  }

  public void bindListener(BatchEventListener listener)
  {
    this.batchListener = listener;
  }

  public void bindPartitionBounds(PartitionBounds bounds)
  {
    this.partitionBounds = bounds;
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return cursor.columns();
  }

  public int offset()
  {
    return 0;
  }

  public void startPartition()
  {
    batchIndex = partitionBounds.startBatch();
    if (bindBatch()) {
      state = State.BEFORE_FIRST;
      positioner.seek(partitionBounds.startRow());
    } else {
      state = State.AFTER_LAST;
    }
  }

  @Override
  public boolean isEOF()
  {
    return eof;
  }

  @Override
  public boolean isValid()
  {
    return state == State.WITHIN_PARITION
        && positioner.isValid();
  }

  @Override
  public boolean next()
  {
    if (state == State.AFTER_LAST) {
      return false;
    } else {
      return nextRow();
    }
  }

  protected boolean nextRow()
  {
    while (true) {
      if (positioner.next()) {
        if (partitionBounds.isWithinPartition(batchIndex, positioner.index())) {
          state = State.WITHIN_PARITION;
          return true;
        } else {
          state = State.AFTER_LAST;
          return false;
        }
      }
      if (!nextBatch()) {
        eof = true;
        state = State.AFTER_LAST;
        return false;
      }
    }
  }

  protected boolean nextBatch()
  {
    if (batchIndex >= 0) {
      // Listeners are not interested in moving off of the -1 bath
      batchListener.exitBatch(batchIndex);
    }
    batchIndex++;
    return bindBatch();
  }

  protected boolean bindBatch()
  {
    Object data = buffer.batch(batchIndex);
    if (data == null && !batchListener.requestBatch(batchIndex)) {
      eof = true;
      return false;
    }
    buffer.inputSchema.type().bindReader(cursor.reader(), buffer.batch(batchIndex));
    return true;
  }

  @Override
  public BatchSchema schema()
  {
    return buffer.inputSchema;
  }

  public static WindowFrameCursor offsetCursor(BatchBuffer buffer, Integer n)
  {
    if (n == 0) {
      return new PrimaryCursor(buffer);
    } else if (n < 0) {
      return new LagCursor(buffer, -n);
    } else {
      return new LeadCursor(buffer, n);
    }
  }

  public static class PrimaryCursor extends WindowFrameCursor
  {
    public PrimaryCursor(BatchBuffer buffer)
    {
      super(buffer);
    }
  }

  public static class LeadCursor extends WindowFrameCursor
  {
    private final int lead;

    public LeadCursor(BatchBuffer buffer, int lead)
    {
      super(buffer);
      this.lead = lead;
    }

    @Override
    public int offset()
    {
      return lead;
    }

    @Override
    public void startPartition()
    {
      super.startPartition();
      int skip = lead;
      while (skip > 0) {
        skip -= partitionBounds.seek(cursor.positioner(), skip);
        if (skip > 0 && !nextBatch()) {
          break;
        }
      }
    }
  }

  public static class LagCursor extends WindowFrameCursor
  {
    private final int lag;
    private int skip;

    public LagCursor(BatchBuffer buffer, int lag)
    {
      super(buffer);
      this.lag = lag;
      this.skip = lag;
    }

    @Override
    public int offset()
    {
      return -lag;
    }

    @Override
    public boolean next()
    {
      if (skip > 0) {
        skip--;
        return true;
      }
      return super.next();
    }
  }
}
