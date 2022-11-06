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

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.util.TypeRegistry;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.List;

/**
 * Divides data into partitions and creates output batches based on
 * the partitioning strategy.
 */
public abstract class Partitioner
{
  /**
   * Partition state used by the reader sequencers to identify
   * partition boundaries.
   */
  public interface State
  {
    boolean isWithinPartition(int batchIndex, int rowIndex);
    int startingBatch();
    int startingRow();
  }

  /**
   * The single partition state for unpartitioned data.
   */
  protected static final State UNPARTITIONED_STATE = new State()
  {
    @Override
    public int startingBatch()
    {
      return -1;
    }

    @Override
    public int startingRow()
    {
      return -1;
    }

    @Override
    public boolean isWithinPartition(int batchIndex, int rowIndex)
    {
      return true;
    }
  };

  protected final BatchBuffer batchBuffer;
  protected final BatchWriter<?> writer;
  protected final List<PartitionSequencer> projectionSources;
  protected final PartitionSequencer primarySequencer;
  protected final RowWriter rowWriter;

  public Partitioner(Builder builder)
  {
    this.batchBuffer = builder.buffer;
    this.writer = builder.writer;
    this.rowWriter = this.writer.rowWriter(builder.projectionBuilder.columnReaders());
    this.projectionSources = builder.sequencers;
    this.primarySequencer = builder.primaryReader.sequencer;
  }

  public abstract void start();
  public abstract int writeBatch();
  public abstract boolean isEOF();

  protected void startPartition()
  {
    for (PartitionSequencer seq : projectionSources) {
      seq.startPartition();
    }
  }

  protected boolean nextInput()
  {
    for (PartitionSequencer seq : projectionSources) {
      seq.next();
    }
    return !primarySequencer.isEOF();
  }

  /**
   * Partitioner for the unpartitioned (i.e. single big partition) case.
   * The bounds of this partition are the start and end of data.
   */
  public static class Single extends Partitioner
  {
    public Single(Builder builder)
    {
      super(builder);
    }

    @Override
    public void start()
    {
      startPartition();
    }

    @Override
    public int writeBatch()
    {
      int rowCount = 0;
      while (!writer.isFull()) {
        if (nextInput()) {
          rowWriter.write();
          rowCount++;
        } else {
          break;
        }
      }
      return rowCount;
    }

    @Override
    public boolean isEOF()
    {
      return primarySequencer.isEOF();
    }
  }

  /**
   * Partitioner for the partitioned case which will typically have multiple
   * partitions. This class acts as a batch loader, so it can intercept calls to
   * load lead batches and identify the partition bounds within each new batch.
   * Tracks the current partition including the start position (which may be
   * far back from the current batch) and end position (which must either be
   * in the current batch, or in some as-yet-unseen future batch.)
   */
  public static class Multiple extends Partitioner implements State, BatchLoader
  {
    private final BatchType batchType;
    private final BatchReader reader;

    /**
     * Column readers for the partition reader: one for each key column.
     */
    private final ScalarColumnReader[] keyColumns;

    /**
     * Comparators for each of the key columns.
     */
    private final Comparator<Object>[] comparators;

    /**
     * The current partition key as read from the key columns.
     */
    private final Object[] currentKey;
    private boolean eof;

    // The current (lead-most) batch.
    private int batchIndex;
    private int rowIndex;

    // The start of the current partition, perhaps on a previous batch.
    private int startBatch = -1;
    private int startOffset = -1;

    // The end of the partition within the current batch. If the same as
    // the batch size, then the partition may continue into the next batch.
    private int endOffset = -1;

    public Multiple(Builder builder)
    {
      super(builder);
      this.batchType = builder.buffer.inputSchema.type();
      this.reader = builder.buffer.inputSchema.newReader();
      this.comparators = TypeRegistry.INSTANCE.ordering(builder.partitionKeys, reader.columns().schema());
      this.keyColumns = new ScalarColumnReader[builder.partitionKeys.size()];
      for (int i = 0; i < keyColumns.length; i++) {
        this.keyColumns[i] = reader.columns().scalar(builder.partitionKeys.get(i));
      }
      this.currentKey = new Object[keyColumns.length];
      for (PartitionSequencer seq : builder.sequencers) {
        seq.bindPartitionState(this);
      }
    }

    @Override
    public void start()
    {
      if (loadBatch() != null) {
        startBatch = 0;
        seekTo(0);
        enterPartition();
      }
    }

    private void enterPartition()
    {
      loadPartitionKey();
      findPartitionEndWithinBatch();
      startPartition();
    }

    /**
     * Call-back from the lead reader sequencer to ask for a new batch.
     * That batch will always be the current batch (on startup) or one
     * beyond the current batch (thereafter).
     */
    @Override
    public Object loadBatch(int batchIndex)
    {
      if (batchIndex == this.batchIndex + 1) {
        return nextBatch();
      } else if (startBatch <= batchIndex && batchIndex <= this.batchIndex) {
        return batchBuffer.loadBatch(batchIndex);
      } else {
        throw new ISE("Incorrect batch index");
      }
    }

    /**
     * Mechanics to load a batch from the buffer and bind it to the
     * partition key reader, if not EOF.
     */
    private Object loadBatch()
    {
      Object data = batchBuffer.loadBatch(batchIndex);
      if (data == null) {
        eof = true;
       } else {
        batchType.bindReader(reader, data);
      }
      return data;
    }

    private void loadPartitionKey()
    {
      for (int i = 0; i < keyColumns.length; i++) {
        currentKey[i] = keyColumns[i].getValue();
      }
    }

    /**
     * Perform a binary search to find the end of the current
     * partition within the current batch. The starts from the
     * row index, which must part at the first row of the partition
     * (if the present partition starts somewhere within the current
     * batch) or the first row (if this is a new batch for the existing
     * partition.) The result will set the partition end offset to our best
     * guess as to the end: either the last row of this partition (if we
     * find row for another partition), or past the end of the current
     * batch (if all remaining rows are of the same partition.)
     */
    private void findPartitionEndWithinBatch()
    {
      int low = rowIndex;
      int high = reader.size() - 1;
      while (low <= high) {
        seekTo((high + low) / 2);
        int result = compareKeys();
        if (result == 0) {
          low = rowIndex + 1;
        } else {
          high = rowIndex - 1;
        }
      }
      endOffset = high;
    }

    /**
     * Move the partition key reader to the given position within the current
     * batch.
      */
    private void seekTo(int n)
    {
      rowIndex = n;
      reader.updatePosition(rowIndex);
    }

    private int compareKeys()
    {
      for (int i = 0; i < keyColumns.length; i++) {
        int result = comparators[i].compare(currentKey[i], keyColumns[i].getValue());
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }

    /**
     * Move to the next batch of data, if one exists. Do search within that batch
     * to find the end of the current partition, if the new batch contains multiple
     * partitions.
      */
    public Object nextBatch()
    {
      batchIndex++;
      rowIndex = 0;
      Object data = loadBatch();
      if (data != null) {
        findPartitionEndWithinBatch();
      }
      return data;
    }

    /**
     * Move to the next partition in the input, unless we're at EOF.
     * Grabs the key from the first row of the new partition, then looks for
     * the end of the partition within the current batch. Adjusts the partition
     * start boundaries used by sequencers to align themselves before the first
     * row of the new partition.
     */
    private boolean nextPartition()
    {
      if (eof) {
        return false;
      }

      // We cannot have found the end of the current partition if the
      // current partition extends to the end of the batch. Instead,
      // we must be at least one row away from the end.
      Preconditions.checkState(endOffset < reader.size());
      startBatch = batchIndex;

      // Start offset is just before the first row of the new partition,
      // which is to say, on the last row of the old partition, or -1 if
      // the last partition ended at a batch boundary.
      startOffset = endOffset;
      seekTo(endOffset + 1);
      enterPartition();
      return true;
    }

    @Override
    public boolean isWithinPartition(int batchIndex, int rowIndex)
    {
      return batchIndex < this.batchIndex || rowIndex <= endOffset;
    }

    @Override
    public boolean isEOF()
    {
      return eof;
    }

    @Override
    public int startingBatch()
    {
      return startBatch;
    }

    @Override
    public int startingRow()
    {
       return startOffset;
    }

    /**
     * Create an output batch. Writes to the output batch until either the output
     * batch is full, or we reach the end of the input. While writing, we move
     * from one partition to the next. The individual row readers report "EOF" when
     * they reach the end of the current partition. Moving to the next partition
     * resets the readers to start on the next partition. It is thus this class,
     * and not the readers, that report EOF from the input: EOF occurs when there
     * is no next partition to move to.
     */
    @Override
    public int writeBatch()
    {
      if (eof) {
        return 0;
      }
      int rowCount = 0;
      while (!writer.isFull()) {
        if (nextInput()) {
          rowWriter.write();
          rowCount++;
        } else if (nextPartition()) {
          startPartition();
        } else {
          break;
        }
      }
      return rowCount;
    }
  }
}