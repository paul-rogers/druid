/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
  protected final BatchBuffer buffer;
  protected final BatchReader reader;
  protected Partitioner.State partitionState = Partitioner.UNPARTITIONED_STATE;
  protected BatchLoader loader;
  protected BatchUnloader unloader;

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

  public void bindBatchLoader(BatchLoader loader)
  {
    this.loader = loader;
  }

  public void bindBatchUnloader(BatchUnloader unloader)
  {
    this.unloader = unloader;
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
    // Skip over any batches
    int startBatch = partitionState.startingBatch();
    unloader.unloadBatches(batchIndex, startBatch - 1);
    batchIndex = startBatch;
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
    unloader.unloadBatches(batchIndex, batchIndex);
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
    Object data = loader.loadBatch(batchIndex);
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
