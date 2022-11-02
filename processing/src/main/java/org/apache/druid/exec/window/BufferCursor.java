package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.RowReader.RowCursor;

public class BufferCursor implements RowCursor
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

  protected final BatchBuffer2 buffer;
  protected final BatchReader reader;
  private Listener listener = NO_OP_LISTENER;

  // Start positioned before the first batch so that the first fetch
  // moves to the first row of the first batch (or EOF in the limit case)
  protected int batchIndex = -1;
  private boolean eof;

  public BufferCursor(BatchBuffer2 buffer)
  {
    this.buffer = buffer;
    this.reader = buffer.inputSchema.newReader();
  }

  public BatchReader reader()
  {
    return reader;
  }

  public int offset()
  {
    return 0;
  }

  public void bindListener(Listener listener)
  {
    this.listener = listener;
  }

  @Override
  public boolean next()
  {
    if (eof) {
      return false;
    }
    while (true) {
      if (reader.cursor().next()) {
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
    buffer.inputSchema.type().bindReader(reader, buffer.batch(batchIndex));
    return true;
  }

  @Override
  public boolean isEOF()
  {
    return eof;
  }

  @Override
  public boolean isValid()
  {
    return !eof && reader.cursor().isValid();
  }

  public static class LeadBufferCursor extends BufferCursor
  {
    private final int lead;
    private boolean primed = false;

    public LeadBufferCursor(BatchBuffer2 buffer, int lead)
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
          int batchSize = reader.batchCursor().size();
          if (skip < batchSize) {
            reader.batchCursor().seek(skip - 1);
            break;
          }
          skip -= batchSize;
        }
      }
      return super.next();
    }
  }

  public static class LagBufferCursor extends BufferCursor
  {
    private final int lag;
    private int skip;

    public LagBufferCursor(BatchBuffer2 buffer, int lag)
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
