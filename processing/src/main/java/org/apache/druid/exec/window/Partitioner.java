package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.RowCursor;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.util.TypeRegistry;
import org.apache.druid.exec.window.BatchBufferOld.BufferPosition;
import org.apache.druid.exec.window.BatchBufferOld.InputReader;
import org.apache.druid.exec.window.BatchBufferOld.PartitionRange;

import java.util.Comparator;
import java.util.List;

public class Partitioner implements RowCursor
{
  private final InputReader reader;
  private final ScalarColumnReader[] keyColumns;
  private final Comparator<Object>[] comparators;

  public Partitioner(final InputReader reader, final List<String> keys)
  {
    Preconditions.checkArgument(!keys.isEmpty());
    this.reader = reader;
    comparators = TypeRegistry.INSTANCE.ordering(keys, reader.columns().schema());
    keyColumns = new ScalarColumnReader[keys.size()];
    for (int i = 0; i < keyColumns.length; i++) {
      keyColumns[i] = reader.columns().scalar(keys.get(i));
    }
  }

  public PartitionRange nextPartition()
  {
    if (reader.sequencer().isEOF()) {
      return null;
    }
    BufferPosition start = reader.currentRow();
    Object[] key = defineKey();
    while (reader.sequencer().next() && sameKey(key)) {
       // Do nothing
    }
    return new PartitionRange(start, reader.previousRow());
  }

  private Object[] defineKey()
  {
    Object[] key = new Object[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      key[i] = keyColumns[i].getValue();
    }
    return key;
  }

  private boolean sameKey(Object[] keys)
  {
    for (int i = 0; i < keyColumns.length; i++) {
      if (comparators[i].compare(keys[i], keyColumns[i].getValue()) != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return reader.columns();
  }

  @Override
  public RowSequencer sequencer()
  {
    return reader.sequencer();
  }
}
