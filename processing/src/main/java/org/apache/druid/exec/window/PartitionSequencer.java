package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.RowSequencer;

/**
 * Drives a reader within a partition. A partition can span multiple
 * input batches. For memory efficiency, the sequencer can load batches
 * as late as possible (just before the first read of that batch), and
 * unload batches as early as possible (after the last reference to that
 * batch.) All of this leads to somewhat complex logic. The parts that
 * differ per situation are handled as "listener" objects to avoid per-row
 * if-statements.
 * <p>
 * The code would be simpler if all the data were in a single batch, and
 * if there was just one partition. But, since data can arrive in variable-sized
 * batches, and data may be sorted by partition, and batches may arrive streaming
 * (if the data is already in the desired sort order), we have to "feel" our
 * way along, moving forward step-by-step to detect batch and partition boundaries.
 * <p>
 * There is one sequencer for each reader. The reader provides the columns used
 * to project the results. The sequencer tells the reader which row to focus upon.
 * In the window operator, we need one reader per each lead/lag offset, plus a
 * "primary" reader for the current window position. The output row is the
 * combination of projections from each primary, lead and lag reader.
 */
public abstract class PartitionSequencer implements RowSequencer
{
  /**
   * Handles events when first entering, or last leaving, a buffered batch.
   */
  public interface BatchEventListener
  {
    void exitBatch(int batchIndex);
    Object requestBatch(int batchIndex);
  }

  /**
   * Listener for a reader which does nothing special on batch events. This
   * occurs either for a reader that is neither the lead-most or lag-most,
   * or for a reader working against buffered data where we don't need to
   * load and unload batches.

   */
  private class PassiveListener implements BatchEventListener
  {
    @Override
    public void exitBatch(int batchIndex)
    {
    }

    @Override
    public Object requestBatch(int batchIndex)
    {
      return buffer.batch(batchIndex);
    }
  }

  /**
   * Listener for the tail-most reader when performing streaming. Unloads the
   * oldest batch when the sequencer moves past it. Since this is used on the
   * tail-most reader, we know no other sequencer (reader) could possibly
   * reference this tail batch.
   */
  private class TailListener extends PassiveListener
  {
    @Override
    public void exitBatch(int batchIndex)
    {
      buffer.unloadBatch(batchIndex);
    }
  }

  /**
   * Listener for the head-most reader when performing streaming. Loads the
   * newest batch when the sequencer moves onto it. Since this is used on
   * the lead-most reader, we know no other sequencer (reader) could possibly
   * have already loaded this batch.
   */
  private class HeadListener extends PassiveListener
  {
    @Override
    public Object requestBatch(int batchIndex)
    {
      return buffer.loadBatch(batchIndex);
    }
  }

  /**
   * Listener for the case when we have only the primary reader and so the
   * primary reader is both the head-most and tail-most reader. It both loads
   * new batches and unloads old batches.
   */
  private class HeadAndTailListener implements BatchEventListener
  {
    @Override
    public void exitBatch(int batchIndex)
    {
      buffer.unloadBatch(batchIndex);
    }

    @Override
    public Object requestBatch(int batchIndex)
    {
      return buffer.loadBatch(batchIndex);
    }
  }

  protected final BatchBuffer buffer;
  protected final BatchReader reader;
  protected Partitioner.State partitionState = Partitioner.GLOBAL_PARTITION;
  private BatchEventListener batchListener;

  // Initial values of the batch index, batch size, and row index don't
  // matter: we'll soon take the values provided by the partition state.
  // For the very first row, the batch index and row index will be -1.
  // The first call to next() will increment the row index to 0, notice it
  // is the same as the default batch size (0), which triggers us to increment
  // the batch index to 0, which causes us to load the first batch, after
  // which things will whirr along.
  protected int batchIndex;
  protected int batchSize;
  protected int rowIndex;
  protected boolean beforeFirst;
  protected boolean endOfPartition;

  public PartitionSequencer(BatchBuffer buffer, BatchReader reader)
  {
    this.buffer = buffer;
    this.reader = reader;
  }

  public void bindPartitionState(Partitioner.State partitionState)
  {
    this.partitionState = partitionState;
  }

  /**
   * Configure batch events for the streaming case.
   *
   * @param isHead this sequencer is the lead-most one, and should load
   *               batches from upstream as needed
   * @param isTail this sequencer is the lag-most one, and should release
   *               batches from the buffer as it moves off of them
   */
  public void bindBatchListener(boolean isHead, boolean isTail)
  {
    if (isHead && isTail) {
      batchListener = new HeadAndTailListener();
    } else if (isHead) {
      batchListener = new HeadListener();
    } else if (isTail) {
      batchListener = new TailListener();
    } else {
      batchListener = new PassiveListener();
    }
  }

  public abstract void startPartition();

  /**
   * Move to before the first row of the next (or only) partition.
   * At startup, moves to before the first (or only) partition, so that
   * the batch index is -1. Subsequent partitions start at some offset
   * in an already-loaded batch. (The batch had to have been loaded for
   * the partitioner to have found the start of the batch.)
   */
  public void seekToStart()
  {
    batchIndex = partitionState.startingBatch();
    bindBatch();
    rowIndex = partitionState.startingRow();
    beforeFirst = true;
    reader.updatePosition(-1);
    endOfPartition = false;
  }

  @Override
  public boolean next()
  {
    return endOfPartition ? false : nextRow();
  }

  /**
   * Move to the next row, in this batch or the next one, but only if
   * in the same partition.
   */
  protected boolean nextRow()
  {
    // We're advancing the row, so no longer the "dead zone" before
    // the first row.
    beforeFirst = false;

    // Advance within the batch. If we reach the end, fetch the
    // next batch and advance to the first row within that batch.
    // Handle any empty batches by skipping over them.
    while (++rowIndex >= batchSize) {
      if (!nextBatch()) {
        return false;
      }
    }

    // We have a valid row within a valid batch. But, is that row
    // part of the current partition?
    if (!partitionState.isWithinPartition(batchIndex, rowIndex)) {
      endOfPartition = true;
      reader.updatePosition(-1);
      return false;
    }

    // The row is good: have the reader focus on it.
    reader.updatePosition(rowIndex);
    return true;
  }

  protected boolean nextBatch()
  {
    if (batchIndex >= 0) {
      // Listeners are not interested in moving off of the -1 batch
      batchListener.exitBatch(batchIndex);
    }
    batchIndex++;
    return bindBatch();
  }

  /**
   * Bind the sequencer and reader to the current batch, which may be
   * non-existent when positioned before the first batch.
   */
  protected boolean bindBatch()
  {
    // Reset for a new batch. Puts the reader before the
    // first row of the new batch.
    rowIndex = -1;
    reader.updatePosition(-1);

    // Before first batch?
    if (batchIndex == -1) {
      batchSize = 0;
      return true;
    }

    // Fetch the batch, loading it if needed. Returns null at EOF.
    Object data = batchListener.requestBatch(batchIndex);
    if (data == null) {
      // EOF
      endOfPartition = true;
      return false;
    }

    // Have a batch. Bind it.
    buffer.inputSchema.type().bindReader(reader, data);
    batchSize = reader.size();
    return true;
  }

  @Override
  public boolean isEOF()
  {
    return endOfPartition;
  }

  @Override
  public boolean isValid()
  {
    return !beforeFirst && !endOfPartition;
  }

  /**
   * A sequencer for the main row within the window: the row at the
   * current window position during the forward scan.
   */
  public static class PrimarySequencer extends PartitionSequencer
  {
    public PrimarySequencer(BatchBuffer buffer, BatchReader reader)
    {
      super(buffer, reader);
    }

    @Override
    public void startPartition()
    {
      seekToStart();
    }
  }

  /**
   * Sequencer for rows ahead of the primary row: those determined by the
   * LEAD keyword in SQL. At the start of each partition, we have to scan
   * ahead of the primary batch to the lead offset, recognizing that, as
   * we do, we may hit the end of the current partition. As we reach the
   * end of the partition, the lead sequencer will set at EOF (or, more
   * accurately, end-of-partition), waiting for the primary partition
   * sequencer to catch up. During that time, the lead sequencer returns
   * nulls for its columns.
   */
  public static class LeadSequencer extends PartitionSequencer
  {
    private final int lead;

    public LeadSequencer(BatchBuffer buffer, BatchReader reader, int lead)
    {
      super(buffer, reader);
      this.lead = lead;
    }

    @Override
    public void startPartition()
    {
      seekToStart();

      // Iterate to skip the requested number of rows. This is
      // inefficient for large leads. It is needed, however, when
      // looking for the start of the next partition during streaming
      // loading. If the batches are already loaded, then we could seek
      // to the desired position, and and do a binary search to work
      // backwards if that desired position is past the end of the
      // current partition. That's left as an optimization for later.
      for (int skip = lead; skip > 0; ) {
        int available = batchSize - rowIndex - 1;
        if (available > skip) {
          rowIndex += skip;
          reader.updatePosition(rowIndex);
          skip = 0;
        } else {
          skip -= available;
          if (!nextBatch()) {
            break;
          }
        }
      }
    }
  }

  /**
   * Sequencer for rows behind the primary row: those determined by the
   * LAG keyword in SQL. At the start of each partition, we have to "idle"
   * over the lead rows, waiting for the primary sequencer to move along
   * far enough that the lag sequencer can start reading off rows. Until
   * then, the lag sequencer returns null value for each of it columns.
   * By its nature, the lag sequencer will never encounter EOF: the primary
   * sequencer will get there first, which will cause reading to stop.
   */
  public static class LagSequencer extends PartitionSequencer
  {
    private final int lag;
    private int skip;

    public LagSequencer(BatchBuffer buffer, BatchReader reader, int lag)
    {
      super(buffer, reader);
      this.lag = lag;
    }

    @Override
    public void startPartition()
    {
      seekToStart();
      skip = lag;
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
