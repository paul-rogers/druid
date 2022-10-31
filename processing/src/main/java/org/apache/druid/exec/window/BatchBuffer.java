package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.ColumnReaderFactory;
import org.apache.druid.exec.batch.RowReader;
import org.apache.druid.exec.batch.BatchReader.BatchCursor;
import org.apache.druid.exec.batch.RowReader.RowCursor;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.ResultIterator.EofException;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class BatchBuffer
{
  public static final BufferPosition START = new BufferPosition(0, 0);
  public static final BufferPosition UNBOUNDED_END = new BufferPosition(Integer.MAX_VALUE, 0);
  public static final PartitionRange UNBOUNDED = new PartitionRange(START, UNBOUNDED_END);

  protected static class BufferPosition
  {
    public final int batchIndex;
    public final int rowIndex;

    public BufferPosition(int batchIndex, int rowIndex)
    {
      this.batchIndex = batchIndex;
      this.rowIndex = rowIndex;
    }
  }

  public static class PartitionRange
  {
    public final BufferPosition start;
    public final BufferPosition end;

    public PartitionRange(BufferPosition start, BufferPosition end)
    {
      this.start = start;
      this.end = end;
    }

    public boolean inRange(int batchIndex, int rowIndex)
    {
      return batchIndex < end.batchIndex || rowIndex < end.rowIndex;
    }
  }

  public class InputReader implements RowReader, RowCursor
  {
    public BufferPosition currentRow()
    {
      return new BufferPosition(batchCount, reader.batchCursor().index());
    }

    public BufferPosition previousRow()
    {
      if (reader.batchCursor().index() == 0) {
        return new BufferPosition(batchCount - 1, buffer.peekLast().size - 1);
      } else {
        return new BufferPosition(batchCount, reader.batchCursor().index() - 1);
      }
    }

    // BatchReader methods

    @Override
    public ColumnReaderFactory columns()
    {
      return reader.columns();
    }

    @Override
    public RowCursor cursor()
    {
      return this;
    }

    // RowCursor methods

    @Override
    public boolean next()
    {
      while (true) {
        if (reader.cursor().next()) {
          return true;
        }
        if (eof) {
          return false;
        }
        InputBatch batch = loadNextBatch();
        if (batch == null) {
          eof = true;
          return false;
        }
        inputSchema.type().bindReader(reader, batch.data);
      }
    }

    @Override
    public boolean isEOF()
    {
      return eof;
    }
  }

  public static class PartitionCursor implements RowCursor
  {
    private final RowCursor primary;
    private final List<RowCursor> followers;

    public PartitionCursor(final RowCursor primary, final List<RowCursor> followers)
    {
      this.primary = primary;
      this.followers = followers;
    }

    @Override
    public boolean next()
    {
      if (!primary.next()) {
        return false;
      }
      for (RowCursor reader : followers) {
        reader.next();
      }
      return true;
    }

    @Override
    public boolean isEOF()
    {
      return primary.isEOF();
    }
  }

  public class PartitionReader implements RowReader, RowCursor
  {
    private final BatchReader batchReader;
    private PartitionRange range;
    private int batchIndex;
    private Iterator<InputBatch> bufferIter;
    private int batchEnd;
    protected boolean eof;

    public PartitionReader()
    {
      this.batchReader = inputSchema.newReader();
    }

    public void bind(PartitionRange range)
    {
      this.range = range;
      Preconditions.checkArgument(range.start.batchIndex == queueHeadBatchIndex());
      bufferIter = buffer.iterator();
      inputSchema.type().bindReader(batchReader, bufferIter.next());
      batchReader.batchCursor().seek(range.start.rowIndex - 1);
    }

    @Override
    public boolean next()
    {
      BatchCursor cursor = batchReader.batchCursor();
      if (cursor.index() < batchEnd) {
        return cursor.next();
      }
      if (batchIndex == range.end.batchIndex) {
        eof = true;
        return false;
      }
      batchIndex++;
      Preconditions.checkState(bufferIter.hasNext());
      inputSchema.type().bindReader(batchReader, bufferIter.next());
      return cursor.next();
    }

    @Override
    public boolean isEOF()
    {
      return eof;
    }

    @Override
    public ColumnReaderFactory columns()
    {
      return batchReader.columns();
    }

    @Override
    public RowCursor cursor()
    {
       return this;
    }

    public boolean isNull()
    {
      return eof;
    }
  }

  public class LeadReader extends PartitionReader
  {
    private int leadNulls;

    public LeadReader(int lead)
    {
      leadNulls = lead;
    }

    @Override
    public boolean next()
    {
      if (leadNulls > 0) {
        leadNulls--;
        return true;
      }
      return super.next();
    }

    @Override
    public boolean isNull()
    {
      return leadNulls > 0 || eof;
    }
  }

  public class LagReader extends PartitionReader
  {
    public LagReader(int lag)
    {

      // Not very efficient for large lags. Consider doing
      // the math instead.
      for (int i = 0; i < lag; i++) {
        if (!next()) {
          break;
        }
      }
    }
  }

  public static class InputBatch
  {
    public final Object data;
    public final int size;

    public InputBatch(Object data, int size)
    {
      this.data = data;
      this.size = size;
    }
  }

  /**
   * Factory for the holders for input batches.
   */
  private final BatchSchema inputSchema;

  /**
   * Upstream source of batches
   */
  private final ResultIterator<?> inputIter;

  /**
   * Sliding window of retained batches.
   */
  private final Deque<InputBatch> buffer = new LinkedList<>();

  /**
   * Total number of batches read thus far from upstream. The
   * buffer holds the last n of these batches.
   */
  private int batchCount;

  /**
   * Reader for the upstream iterator.
   */

  private BatchReader reader;

  /**
   * Reader used when the input is partitioned, and we must find
   * partition boundaries (i.e., the frame) before computing the rows
   * within a frame.
   */
  private InputReader inputReader;

  /**
   * If EOF was seen from upstream.
   */
  private boolean eof;

  public BatchBuffer(final BatchSchema inputSchema, final ResultIterator<?> inputIter)
  {
    this.inputSchema = inputSchema;
    this.inputIter = inputIter;
  }

  /**
   * Initialize the buffer.
   *
   * @return {@code true} if there is at least one row available from
   * upstream, {@code false} if the result set is empty.
   */
  public boolean open()
  {
    Preconditions.checkState(buffer.isEmpty());
    return loadNextBatch() != null;
  }

  private int queueHeadBatchIndex()
  {
    return batchCount - buffer.size();
  }

  /**
   * Load the next non-empty batch from upstream and add it to the
   * queue.
   *
   * @return the batch, else {@code null} at EOF.
   */
  private InputBatch loadNextBatch()
  {
    while (true) {
      Object data;
      try {
        data = inputIter.next();
      } catch (EofException e) {
        return null;
      }
      int size = inputSchema.type().sizeOf(data);
      if (size > 0) {
        batchCount++;
        InputBatch batch = new InputBatch(data, size);
        buffer.add(batch);
        return batch;
      }
    }
  }

  /**
   * Load all rows: needed for an aggregate over an unbounded frame.
   */
  public void loadUnboundedFrame()
  {
    while (loadNextBatch() != null) {
      // Empty
    }
  }

  /**
   * Return a batch reader to be used to find partition boundaries
   * in sorted input.
   */
  public InputReader inputReader()
  {
    if (inputReader == null) {
      inputReader = new InputReader();
    }
    return inputReader;
  }

  public PartitionReader primaryReader()
  {
    return new PartitionReader();
  }

  public PartitionReader offsetReader(int offset)
  {
    if (offset < 0) {
      return new LeadReader(-offset);
    } else {
      return new LagReader(offset);
    }
  }
}
