package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.util.TypeRegistry;

import java.util.Comparator;
import java.util.List;

public abstract class Partitioner
{
  public interface State
  {
    boolean isWithinPartition(int batchIndex, int rowIndex);
    int startingBatch();
    int startingRow();
  }

  public static final State GLOBAL_PARTITION = new State()
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
  protected final RowWriter rowWriter;

  public Partitioner(Builder builder)
  {
    this.batchBuffer = builder.buffer;
    this.writer = builder.writer;
    this.rowWriter = this.writer.rowWriter(builder.projectionBuilder.columnReaders());
    this.projectionSources = builder.sequencers;
  }

  public abstract int writeBatch();
  public abstract boolean isEOF();

  protected void advance()
  {
    for (PartitionSequencer seq : projectionSources) {
      seq.next();
    }
  }

  protected void startPartition()
  {
    for (PartitionSequencer seq : projectionSources) {
      seq.startPartition();
    }
  }

  public static class Single extends Partitioner
  {
    private final PartitionSequencer primary;

    public Single(Builder builder)
    {
      super(builder);
      this.primary = builder.primaryReader.sequencer;
      startPartition();
    }

    @Override
    public int writeBatch()
    {
      int rowCount = 0;
      while (!writer.isFull() && !isEOF()) {
        while (nextInput()) {
          rowWriter.write();
          rowCount++;
        }
      }
      return rowCount;
    }

    private boolean nextInput()
    {
      advance();
      return !primary.isEOF();
    }

    @Override
    public boolean isEOF()
    {
      return primary.isEOF();
    }
  }

  public static class Multiple extends Partitioner implements Partitioner.State
  {
    private final BatchReader reader;
    private final PartitionSequencer sequencer;
    private final ScalarColumnReader[] keyColumns;
    private final Comparator<Object>[] comparators;
    private final Object[] currentKey;
    private int startBatch;
    private int startOffset;
    private int endBatch;
    private int endOffset;

    public Multiple(Builder builder)
    {
      super(builder);
      this.reader = builder.buffer.inputSchema.newReader();
      this.sequencer = new PartitionSequencer.PrimarySequencer(builder.buffer, reader);
      this.comparators = TypeRegistry.INSTANCE.ordering(builder.partitionKeys, reader.columns().schema());
      this.keyColumns = new ScalarColumnReader[builder.partitionKeys.size()];
      for (int i = 0; i < keyColumns.length; i++) {
        this.keyColumns[i] = reader.columns().scalar(builder.partitionKeys.get(i));
      }
      this.currentKey = new Object[keyColumns.length];
      for (PartitionSequencer seq : builder.sequencers) {
        seq.bindPartitionState(this);
      }
      startPartition();
    }

    @Override
    protected void startPartition()
    {
      for (int i = 0; i < keyColumns.length; i++) {
        currentKey[i] = keyColumns[i].getValue();
      }
      super.startPartition();
    }

    @Override
    public int writeBatch()
    {
      int rowCount = 0;
      while (!writer.isFull()) {
        while (sequencer.next()) {
          rowWriter.write();
          rowCount++;
        }
        if (sequencer.isEOF()) {
          break;
        }
        startNextPartition();
        sequencer.startPartition();
      }
      return rowCount;
    }

    public boolean isSamePartition()
    {
      for (int i = 0; i < keyColumns.length; i++) {
        if (comparators[i].compare(currentKey[i], keyColumns[i].getValue()) != 0) {
          endBatch = sequencer.batchIndex;
          endOffset = sequencer.rowIndex;
          return false;
        }
      }
      return true;
    }

    public void markPartitionStart()
    {
      startBatch = sequencer.batchIndex;
      startOffset = sequencer.rowIndex;
    }

    public void startNextPartition()
    {
      startBatch = endBatch;
      startOffset = endOffset;
      endBatch = -1;
      endOffset = -1;
    }

    @Override
    public boolean isWithinPartition(int batchIndex, int rowIndex)
    {
      return endBatch == -1 || (batchIndex <= endBatch && rowIndex <= endOffset);
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

    @Override
    public boolean isEOF()
    {
      return sequencer.isEOF();
    }
  }
}