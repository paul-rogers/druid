package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.batch.RowReader.RowCursor;
import org.apache.druid.exec.window.BatchBuffer.PartitionRange;
import org.apache.druid.exec.window.BatchBuffer.PartitionReader;

import java.util.List;

public class Projector
{
  private final RowCursor inputCursor;
  private final List<PartitionReader> readers;
  private final BatchWriter<?> output;
  private final RowWriter rowWriter;

  public Projector(
      final RowCursor inputCursor,
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
