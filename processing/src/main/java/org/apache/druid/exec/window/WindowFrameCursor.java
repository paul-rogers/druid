package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.RowCursor;
import org.apache.druid.exec.batch.RowSequencer;

public class WindowFrameCursor implements RowCursor, RowSequencer
{
  public interface Listener
  {
    void exitBatch(int batchIndex);
    boolean requestBatch(int batchIndex);
  }

  private static final Listener NO_OP_LISTENER = new Listener()
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

  protected final BatchBuffer buffer;
  protected final BatchCursor cursor;
  private Listener listener = NO_OP_LISTENER;

  // Start positioned before the first batch so that the first fetch
  // moves to the first row of the first batch (or EOF in the limit case)
  protected int batchIndex = -1;
  protected boolean eof;

  public WindowFrameCursor(BatchBuffer buffer)
  {
    this.buffer = buffer;
    this.cursor = Batches.toCursor(buffer.inputSchema.newReader());
  }

  public void bindListener(Listener listener)
  {
    this.listener = listener;
  }

//  public PositionListener wrapListener(PositionListener wrapper)
//  {
//    SimpleBatchPositioner positioner = (SimpleBatchPositioner) cursor.sequencer();
//    PositionListener oldListener = positioner.listener();
//    positioner.bindListener(wrapper);
//    return oldListener;
//  }

  @Override
  public ColumnReaderProvider columns()
  {
    return cursor.columns();
  }

  public int offset()
  {
    return 0;
  }

  @Override
  public boolean isEOF()
  {
    return eof;
  }

  @Override
  public boolean isValid()
  {
    return !eof && cursor.positioner().isValid();
  }

  @Override
  public boolean next()
  {
    if (eof) {
      return false;
    }
    return nextRow();
  }

  protected boolean nextRow()
  {
    while (true) {
      if (cursor.positioner().next()) {
        return true;
      }
      if (!nextBatch()) {
        return false;
      }
    }
  }

  protected boolean nextBatch()
  {
    if (batchIndex >= 0) {
      // Listeners are not interested in moving off of the -1 bath
      listener.exitBatch(batchIndex);
    }
    batchIndex++;
    Object data = buffer.batch(batchIndex);
    if (data == null && !listener.requestBatch(batchIndex)) {
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
      return new UnboundedCursor(buffer);
    } else if (n < 0) {
      return new UnboundedLagCursor(buffer, -n);
    } else {
      return new UnboundedLeadCursor(buffer, n);
    }
  }

  public static class UnboundedCursor extends WindowFrameCursor
  {
    public UnboundedCursor(BatchBuffer buffer)
    {
      super(buffer);
    }
  }

  public static class UnboundedLeadCursor extends WindowFrameCursor
  {
    private final int lead;
    private boolean primed = false;

    public UnboundedLeadCursor(BatchBuffer buffer, int lead)
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
    public boolean next()
    {
      if (!primed) {
        primed = true;
        int skip = lead;
        while (true) {
          if (!nextBatch()) {
            return false;
          }
          int batchSize = cursor.positioner().size();
          if (skip < batchSize) {
            cursor.positioner().seek(skip - 1);
            break;
          }
          skip -= batchSize;
        }
      }
      return super.next();
    }
  }
  public static class UnboundedLagCursor extends WindowFrameCursor
  {
    private final int lag;
    private int skip;

    public UnboundedLagCursor(BatchBuffer buffer, int lag)
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
