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

  private final BatchBuffer2 buffer;
  private final BatchReader reader;
  private Listener listener = NO_OP_LISTENER;
  private final int offset;
  private int lag;
  private int batchIndex;
  private boolean eof;

  public BufferCursor(BatchBuffer2 buffer, int offset)
  {
    this.buffer = buffer;
    this.reader = buffer.inputSchema.newReader();
    this.offset = offset;
    this.lag = Math.max(0, -offset);
    buffer.inputSchema.type().bindReader(reader, buffer.batch(batchIndex));
  }

  public int offset()
  {
    return offset;
  }

  public void bindListener(Listener listener)
  {
    this.listener = listener;
  }

  @Override
  public boolean next()
  {
    if (lag > 0) {
      lag--;
      return true;
    }
    if (eof) {
      return false;
    }
    while (true) {
      if (reader.cursor().next()) {
        return true;
      }
      listener.exitBatch(batchIndex);
      batchIndex++;
      Object data = buffer.batch(batchIndex);
      if (data == null && !listener.requestBatch(batchIndex)) {
        eof = true;
        return false;
      }
      buffer.inputSchema.type().bindReader(reader, buffer.batch(batchIndex));
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
    return !eof && reader.cursor().isValid();
  }
}
