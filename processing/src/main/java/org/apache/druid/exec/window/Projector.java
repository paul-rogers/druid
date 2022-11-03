package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSequencer;
import org.apache.druid.exec.window.BatchBufferOld.PartitionRange;
import org.apache.druid.exec.window.BatchBufferOld.PartitionReader;

import java.util.List;

public class Projector
{
  private final RowSequencer inputCursor;
  private final List<PartitionReader> readers;
  private final BatchWriter<?> output;
  private final RowWriter rowWriter;

  public Projector(
      final RowSequencer inputCursor,
      final List<PartitionReader> readers,
      final BatchWriter<?> output,
      final List<ScalarColumnReader> colReaders
  )
  {
    this.inputCursor = inputCursor;
    this.readers = readers;
    this.output = output;
    this.rowWriter = output.rowWriter(colReaders);
  }

  public void bindToPartition(PartitionRange range)
  {
    for (PartitionReader reader : readers) {
      reader.bind(range);
    }
  }

  public boolean isBatchFull()
  {
    return output.isFull();
  }

  public boolean project()
  {
    if (!inputCursor.next()) {
      return false;
    }
    if (output.isFull()) {
      return false;
    }
    rowWriter.write();
    return true;
  }
}
