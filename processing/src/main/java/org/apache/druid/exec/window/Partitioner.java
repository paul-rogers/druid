package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.util.TypeRegistry;
import org.apache.druid.utils.CollectionUtils;

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

  public static class Builder
  {
    private final BatchBuffer batchBuffer;
    private final List<String> partitionKeys;
    private final BatchWriter<?> writer;
    private List<PartitionSequencer> sequencers;
    private List<ScalarColumnReader> columnReaders;

    public Builder(BatchBuffer batchBuffer, List<String> partitionKeys, BatchWriter<?> writer)
    {
      this.batchBuffer = batchBuffer;
      this.partitionKeys = partitionKeys;
      this.writer = writer;
    }

    public Partitioner build(List<PartitionSequencer> sequencers, List<ScalarColumnReader> columnReaders)
    {
      this.sequencers = sequencers;
      this.columnReaders = columnReaders;
      if (isPartitioned()) {
        return new Multiple(this);
      } else {
        return new Single(this);
      }
    }

    public boolean isPartitioned()
    {
      return !CollectionUtils.isNullOrEmpty(partitionKeys);
    }
  }

  protected final BatchBuffer batchBuffer;
  protected final BatchWriter<?> writer;
  protected final List<PartitionSequencer> projectionSources;
  protected RowWriter rowWriter;

  public Partitioner(Partitioner.Builder builder)
  {
    this.batchBuffer = builder.batchBuffer;
    this.writer = builder.writer;
    this.rowWriter = this.writer.rowWriter(builder.columnReaders);
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
  public static class Single extends Partitioner
  {
    private PartitionSequencer primary;

    public Single(Partitioner.Builder builder)
    {
      super(builder);
      this.primary = projectionSources.get(0);
    }

    @Override
    public int writeBatch()
    {
      int rowCount = 0;
      while (!writer.isFull()) {
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

    public Multiple(Partitioner.Builder builder)
    {
      super(builder);
      this.reader = builder.batchBuffer.inputSchema.newReader();
      this.sequencer = new PartitionSequencer.PrimarySequencer(builder.batchBuffer, reader);
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

    public void startPartition()
    {
      for (int i = 0; i < keyColumns.length; i++) {
        currentKey[i] = keyColumns[i].getValue();
      }
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